// Regenerates the `gold_options/` data inside every MOR fixture: what Hudi's
// own reader returns for a set of column projections, plus the manifest
// recording exactly which projections produced them.
//
// The manifest is the contract between this script and
// `crates/core/tests/gold_parity_tests.rs`, and this script is its ONLY author.
// It derives each case from the fixture's own schema and `hoodie.properties`,
// then writes down what it selected; the Rust sweep replays that record rather
// than re-deriving it. Nothing is expressed twice, so the two cannot come to
// disagree about what a case means.
//
// Run against a directory holding one unzipped fixture per subdirectory, each
// laid out the way the zip is — the table directory and `gold_data/` as
// siblings:
//
//   $WORK_ROOT/table_parquet_log_block/table_parquet_log_block/  <- the table
//   $WORK_ROOT/table_parquet_log_block/gold_data/
//   $WORK_ROOT/table_parquet_log_block/gold_options/             <- written here
//
//   export WORK_ROOT=<that directory>
//   export FIXTURES=<optional comma-separated subset; default every subdir>
//   $SPARK_HOME/bin/spark-shell --master 'local[2]' \
//     --jars <hudi-spark3.5-bundle>.jar \
//     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
//     --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
//     --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
//     -i gold_options.scala
//
// Generated with Spark 3.5.3 and Hudi 1.2.0-SNAPSHOT.

import java.io.{File, FileInputStream, PrintWriter}
import java.util.Properties

import org.apache.spark.sql.types.{ArrayType, MapType, StructType}

val workRoot = new File(sys.env("WORK_ROOT"))
val onlyFixtures: Set[String] =
  sys.env.get("FIXTURES").map(_.split(",").map(_.trim).filter(_.nonEmpty).toSet).getOrElse(Set.empty)

/** Hudi metadata columns are excluded from every projection case. */
val MetaPrefix = "_hoodie_"

def loadProps(tablePath: File): Properties = {
  val props = new Properties()
  val in = new FileInputStream(new File(tablePath, ".hoodie/hoodie.properties"))
  try props.load(in) finally in.close()
  props
}

def csvProp(props: Properties, keys: String*): Seq[String] =
  keys.flatMap(k => Option(props.getProperty(k))).headOption.toSeq
    .flatMap(_.split(",").map(_.trim).filter(_.nonEmpty))

/** The record key column(s). Always present. */
def recordKeyFields(props: Properties): Seq[String] =
  csvProp(props, "hoodie.table.recordkey.fields")

/**
 * The ordering (precombine) column(s), or empty when the table declares none.
 *
 * The property was renamed at table version 9; v6/v8 fixtures still spell it
 * `precombine.field`. Several fixtures declare neither, which is why
 * `drop_ordering` is a case that can legitimately be absent from a manifest.
 */
def orderingFields(props: Properties): Seq[String] =
  csvProp(props, "hoodie.table.ordering.fields", "hoodie.table.precombine.field")

/**
 * Completion times of the fixture's completed commits, in timeline order.
 *
 * A completed instant is named `<requested>_<completion>.<action>` under
 * timeline layout v2 (table version 8+) and plain `<requested>.<action>` before
 * it, so the completion time is the half after the underscore where there is
 * one. Inflight and requested markers are excluded — an incremental read only
 * ever sees completed commits.
 */
def completionTimes(tablePath: File): Seq[String] = {
  val v2 = new File(tablePath, ".hoodie/timeline")
  val dir = if (v2.isDirectory) v2 else new File(tablePath, ".hoodie")
  val completed = Option(dir.listFiles).getOrElse(Array.empty[File])
    .map(_.getName)
    .filter(n => n.endsWith(".commit") || n.endsWith(".deltacommit") || n.endsWith(".replacecommit"))
  completed
    .map { name =>
      val stem = name.substring(0, name.lastIndexOf('.'))
      val parts = stem.split("_")
      (parts.head, parts.last) // (requested, completion); equal pre-layout-v2
    }
    .sortBy(_._1)
    .map(_._2)
    .toSeq
}

/**
 * The incremental windows for one fixture.
 *
 * Both windows open at `0` — before any commit — rather than at a commit's own
 * timestamp. That is deliberate. Hudi ranges incremental reads on a commit's
 * COMPLETION time and includes the lower bound; hudi-rs ranges on its REQUESTED
 * time and excludes it. A window whose start sits on a commit boundary
 * therefore disagrees by construction on every table version 8+ fixture, which
 * would bury the sweep in dozens of entries for one already-known difference.
 * The `v8_mor_boundary_windows` fixture probes that difference deliberately and
 * is where it belongs.
 *
 * Opening at `0` and varying only the END bound still exercises the incremental
 * planning path across the corpus, and still asks a real question: the second
 * window must drop exactly the last commit's changes.
 */
def deriveWindows(completions: Seq[String]): Seq[(String, String, String)] = {
  // Hudi validates a query instant: it must parse as a date (14 or 17 digits)
  // or be one of the bootstrapping sentinels, so `"0"` and a row of nines are
  // both rejected outright and the bounds have to be real instants.
  //
  // `HoodieTimeline.INIT_INSTANT_TS` ("00000000000000") would be the idiomatic
  // "before everything", and Hudi whitelists it — but hudi-rs rejects it, so it
  // cannot be used here. Its parser falls back to epoch millis only for a
  // 17-character value (for metadata-table instants like 17 zeros), leaving the
  // 14-character sentinel matching neither branch. An epoch-zero date works on
  // both sides and orders the same way.
  val beforeAll = "19700101000000"
  val afterAll = "99991231235959999"
  val full = Seq(("incr_all", beforeAll, afterAll))
  if (completions.length < 2) full
  else full :+ ("incr_through_penultimate", beforeAll, completions(completions.length - 2))
}

/**
 * The projection cases for one fixture, in a stable order.
 *
 * A case whose column list would be empty is dropped rather than emitted, so a
 * fixture with no ordering field simply has no `drop_ordering` case. Cases that
 * would select an identical column list collapse to the first — distinct names
 * for the same read buy no coverage and only inflate the fixture.
 */
def deriveCases(
    cols: Seq[String],
    nested: Seq[String],
    keys: Seq[String],
    ordering: Seq[String]): Seq[(String, Seq[String])] = {
  val keyCols = keys.filter(cols.contains)
  val orderCols = ordering.filter(cols.contains)
  val candidates = Seq(
    // The narrowest read: everything but the key is pruned.
    "key_only" -> keyCols,
    // Structural for merge-on-read: the merge needs the key internally, so this
    // asserts the reader strips it AFTER merging rather than never reading it.
    "drop_key" -> cols.filterNot(keyCols.contains),
    // Likewise for the ordering column, which the merge needs to pick a winner.
    "drop_ordering" -> (if (orderCols.isEmpty) Seq.empty else cols.filterNot(orderCols.contains)),
    // Output column order must follow the request, not the schema.
    "reordered" -> (if (cols.length > 1) cols.reverse else Seq.empty),
    // A single column that is neither key nor ordering field.
    "single_col" -> cols.find(c => !keyCols.contains(c) && !orderCols.contains(c)).toSeq,
    // Container columns only, where the fixture has any.
    "nested_only" -> nested)

  val seen = scala.collection.mutable.Set.empty[Seq[String]]
  candidates.filter { case (_, projection) =>
    projection.nonEmpty && seen.add(projection)
  }
}

def jsonEscape(s: String): String = s.flatMap {
  case '"'  => "\\\""
  case '\\' => "\\\\"
  case '\n' => "\\n"
  case c    => c.toString
}

/**
 * One case: a name plus the read options that produce its gold. Only the
 * options a case is about are set; the rest stay at their defaults and are
 * omitted from the manifest entirely.
 */
case class Case(
    name: String,
    projection: Seq[String] = Seq.empty,
    readOptimized: Boolean = false,
    startTs: Option[String] = None,
    endTs: Option[String] = None)

def writeManifest(dir: File, fixture: String, cases: Seq[Case]): Unit = {
  val entries = cases.map { c =>
    val fields = scala.collection.mutable.ArrayBuffer(s""""name": "${jsonEscape(c.name)}"""")
    if (c.projection.nonEmpty) {
      val cols = c.projection.map(x => "\"" + jsonEscape(x) + "\"").mkString(", ")
      fields += s""""projection": [$cols]"""
    }
    if (c.readOptimized) fields += """"read_optimized": true"""
    c.startTs.foreach(t => fields += s""""start_timestamp": "${jsonEscape(t)}"""")
    c.endTs.foreach(t => fields += s""""end_timestamp": "${jsonEscape(t)}"""")
    s"""    { ${fields.mkString(", ")} }"""
  }
  val json =
    s"""{
       |  "fixture": "${jsonEscape(fixture)}",
       |  "cases": [
       |${entries.mkString(",\n")}
       |  ]
       |}
       |""".stripMargin
  val out = new PrintWriter(new File(dir, "manifest.json"), "UTF-8")
  try out.write(json) finally out.close()
}

val roots = workRoot.listFiles.filter(_.isDirectory).sortBy(_.getName)
  .filter(d => onlyFixtures.isEmpty || onlyFixtures.contains(d.getName))

var totalCases = 0
for (root <- roots) {
  val fixture = root.getName
  val tablePath = new File(root, fixture)
  if (!new File(tablePath, ".hoodie").isDirectory) {
    println(s"GOLD_OPTIONS_SKIP\t$fixture\tno .hoodie under ${tablePath.getPath}")
  } else {
    val props = loadProps(tablePath)
    val df = spark.read.format("hudi").load(tablePath.getPath)

    val userFields = df.schema.fields.filterNot(_.name.startsWith(MetaPrefix))
    val cols = userFields.map(_.name).toSeq
    val nested = userFields.filter(_.dataType match {
      case _: ArrayType | _: MapType | _: StructType => true
      case _ => false
    }).map(_.name).toSeq

    val projectionCases = deriveCases(cols, nested, recordKeyFields(props), orderingFields(props))
      .map { case (name, projection) => Case(name, projection = projection) }
    val windowCases = deriveWindows(completionTimes(tablePath)).map {
      case (name, start, end) => Case(name, startTs = Some(start), endTs = Some(end))
    }
    val candidates = projectionCases ++ Seq(Case("read_optimized", readOptimized = true)) ++
      windowCases

    val goldOptions = new File(root, "gold_options")
    goldOptions.mkdirs()

    // A case that Hudi refuses to read at all is dropped with a loud line rather
    // than silently: the manifest is authoritative, so an absent case is absent
    // coverage and must be visible in the generator's output.
    val written = candidates.flatMap { c =>
      // Gold carries the user columns only: the sweep never compares `_hoodie_`
      // columns, and keeping them would bloat every fixture.
      val selected = if (c.projection.nonEmpty) c.projection else cols
      try {
        // Inside the try: Hudi rejects some reads at load time (an unsupported
        // query type for the table, say), and that must skip the case rather
        // than abort the whole run.
        val loaded =
          if (c.readOptimized) {
            spark.read.format("hudi")
              .option("hoodie.datasource.query.type", "read_optimized")
              .load(tablePath.getPath)
          } else if (c.startTs.isDefined) {
            spark.read.format("hudi")
              .option("hoodie.datasource.query.type", "incremental")
              .option("hoodie.datasource.read.begin.instanttime", c.startTs.get)
              .option("hoodie.datasource.read.end.instanttime", c.endTs.get)
              .load(tablePath.getPath)
          } else df

        loaded.select(selected.head, selected.tail: _*)
          .coalesce(1)
          .write.mode("overwrite")
          .parquet(new File(goldOptions, c.name).getPath)
        println(s"GOLD_OPTION\t$fixture\t${c.name}\t${selected.mkString(",")}")
        totalCases += 1
        Some(c)
      } catch {
        case e: Exception =>
          println(s"GOLD_OPTIONS_SKIP\t$fixture\t${c.name}\t${e.getClass.getName}: ${e.getMessage}")
          None
      }
    }
    val cases = written
    writeManifest(goldOptions, fixture, cases)
    println(s"GOLD_OPTIONS_DONE\t$fixture\tcases=${cases.length}")
  }
}
println(s"GOLD_OPTIONS_TOTAL\tfixtures=${roots.length}\tcases=$totalCases")

System.exit(0)
