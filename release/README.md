<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Release Guide

## Start a tracking issue

```markdown
*This issue is for tracking tasks of releasing `hudi-rs` {version}.*

## Tasks

### Issues

- [ ] All remaining issues in the [milestone](https://github.com/apache/hudi-rs/milestone/1) should be closed.

> ![IMPORTANT]
> Blockers to highlight

- [ ] https://github.com/apache/hudi-rs/issues/41
- [ ] https://github.com/apache/hudi-rs/issues/42

### GitHub

- [ ] Bump version
- [ ] Push release tag

### ASF

- [ ] Create an ASF release
- [ ] Upload artifacts to the SVN dist repo
- [ ] Start VOTE in dev email list

> ![IMPORTANT]
> Proceed from here only after VOTE passes.

### Official release

- [ ] Push the release git tag
- [ ] Publish artifacts to SVN RELEASE branch
- [ ] Send `ANNOUNCE` email to dev and user email lists
```

## GitHub work

### Bump version

> ![NOTE]
> We adhere to [Semantic Versioning](https://semver.org/), and create a release branch for each major or minor release.

For a major or minor release, create a release branch in the format of `release/[0-9]+.[0-9]+.x` matching the target
release version. For example, if it's `0.2.0`, cut a branch named `release/0.2.x`, if it's `1.0.0`, cut a branch
named `release/1.0.x`.

For a patch release, don't cut a new branch, use its base release branch. For example, if it's `0.3.1`, cherry-pick
fixes to `release/0.3.x`.

Create and merge a PR to bump the major or minor version on the `main` branch. For example, if the current version
is `0.3.0`, bump it to `0.4.0` or `1.0.0`.

On the release branch, bump the version to indicate pre-release by pushing a commit.

- If it is meant for internal testing, go with `alpha.1`, `alpha.2`, etc
- If it is meant for public testing, go with `beta.1`, `beta.2`, etc
- If it is ready for voting, go with `rc.1`, `rc.2`, etc

### Testing

Once the "bump version" commit is pushed, CI will be triggered and all tests need to pass before proceed to the next.

### Push tag

> ![IMPORTANT]
> The version in the code should be the target release version.

Push a tag to the commit that matches to the version in the form of `release-*`. For example,

- If the release is `0.1.0-rc.1`, the tag should `release-0.1.0-rc.1`
- If the release is `2.0.0-beta.2`, the tag should `release-2.0.0-beta.2`

> ![CAUTION]
> Pushing a matching tag to the upstream (apache) branch will trigger CI to publish the artifacts to crates.io and
> pypi.org, which, if successful, is irreversible. Same versions are not allowed to publish more than once.

Once the CI completes, check crates.io and pypi.org for the new release artifacts.

## ASF work

### Upload source release

Run the below script to create source release artifacts.

```shell
./release/create_src_release.sh
```

Upload source release artifacts to SVN (dev).

TODO: WIP

### Start a `[VOTE]` thread

```text
[VOTE] hudi-rs 0.1.0, release candidate #2

Hi everyone,

Please review and vote on hudi-rs 0.1.0-rc.2 as follows:

[ ] +1, Approve the release
[ ] -1, Do not approve the release (please provide specific comments)

The complete staging area is available for you to review:

* Release tracking issue is up-to-date [1]
* Categorized changelog for this release [2]
* Source release has been deployed to dist.apache.org [3]
* Source release can be verified using this script [4]
* Source code commit is tagged as "release-0.1.0-rc.2" [5]
* Source code commit CI has passed [6]
* Python artifacts have been published to pypi.org [7]
* Rust artifacts have been published to crates.io [8]

The vote will be open for at least 72 hours. It is adopted by majority
approval, with at least 3 PMC affirmative votes.

Thanks,
Release Manager

[1] https://github.com/apache/hudi-rs/issues/62
[2] https://github.com/apache/hudi-rs/issues/62#issuecomment-2224322166
[3] https://dist.apache.org/repos/dist/dev/hudi/hudi-rs-0.1.0-rc.2/
[4] https://github.com/apache/hudi-rs/blob/7b2d199c180bf36e2fac5e03559fffbfe00bf5fe/release/verify_src_release.sh
[5] https://github.com/apache/hudi-rs/releases/tag/release-0.1.0-rc.2
[6] https://github.com/apache/hudi-rs/actions/runs/9901188924
[7] https://pypi.org/project/hudi/0.1.0rc2/
[8] https://crates.io/crates/hudi/0.1.0-rc.2
```