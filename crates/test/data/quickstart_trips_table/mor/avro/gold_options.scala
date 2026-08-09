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

def writeManifest(dir: File, fixture: String, cases: Seq[(String, Seq[String])]): Unit = {
  val entries = cases.map { case (name, projection) =>
    val cols = projection.map(c => "\"" + jsonEscape(c) + "\"").mkString(", ")
    s"""    { "name": "${jsonEscape(name)}", "projection": [$cols] }"""
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

    val cases = deriveCases(cols, nested, recordKeyFields(props), orderingFields(props))
    val goldOptions = new File(root, "gold_options")
    goldOptions.mkdirs()

    for ((name, projection) <- cases) {
      df.select(projection.head, projection.tail: _*)
        .coalesce(1)
        .write.mode("overwrite")
        .parquet(new File(goldOptions, name).getPath)
      println(s"GOLD_OPTION\t$fixture\t$name\t${projection.mkString(",")}")
      totalCases += 1
    }
    writeManifest(goldOptions, fixture, cases)
    println(s"GOLD_OPTIONS_DONE\t$fixture\tcases=${cases.length}")
  }
}
println(s"GOLD_OPTIONS_TOTAL\tfixtures=${roots.length}\tcases=$totalCases")

System.exit(0)
