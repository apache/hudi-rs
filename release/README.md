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

Create a tracking issue for the release with title `Tracking issue for release x.y.z`. Add the issue to the
corresponding milestone for the release, and edit the issue description as below.

```markdown
## Tasks

This issue tracks the release process as instructed in the [release guide](https://github.com/apache/hudi-rs/tree/main/release/README.md).

### Issues

- [ ] All other issues in the [release milestone](https://github.com/apache/hudi-rs/milestone/xxx) should be closed.

> [!IMPORTANT]
> Highlight blocker issues if any

- [ ] https://github.com/apache/hudi-rs/issues/xxx
- [ ] https://github.com/apache/hudi-rs/issues/xxx

### Prepare release candidate(s)

- [ ] Create a release branch 
- [ ] Bump the version in the release branch for the target RC
- [ ] Bump the version in `main` branch
- [ ] Upload the target RC artifacts to the ASF dev repo (SVN)
- [ ] Verify the target RC artifacts
- [ ] Push a desired RC release git tag to the release branch
- [ ] Verify the published crates.io and pypi.org artifacts
- [ ] Start VOTE in `dev@hudi.apache.org`

> [!IMPORTANT]
> Proceed from here only after VOTE passes.

### Official release

- [ ] Bump the version in the release branch for the official release
- [ ] Push the official release git tag to the release branch
- [ ] Verify the published crates.io and pypi.org artifacts
- [ ] Upload the release artifacts to the ASF release repo (SVN)
- [ ] Merge a PR to update the changelog in `main` branch
- [ ] Publish release notes in https://github.com/apache/hudi-rs/releases
- [ ] Register ASF release in https://reporter.apache.org/addrelease.html?hudi
- [ ] Send `ANNOUNCE` email to dev and user email lists
- [ ] Close this tracking issue and the release milestone
```

## Prepare release candidate(s)

> [!NOTE]
> We adhere to [Semantic Versioning](https://semver.org/), and create a release branch for each major or minor release.

### Bump version in main branch

Execute the below script that creates a branch with the new version changes committed. Submit a PR using that.

```shell
./release/bump_version_in_main.sh
```

### Bump version in release branch

> [!IMPORTANT]
> When working on a release branch, use a local clone of `apache/hudi-rs` instead of your own fork.

For a major or minor release, create a release branch in the format of `release/[0-9]+.[0-9]+.x` matching the target
release version. For example, if it's `0.2.0`, cut a branch named `release/0.2.x`, if it's `1.0.0`, cut a branch
named `release/1.0.x`.

For a patch release, don't cut a new branch, use its base release branch. For example, if it's `0.3.1`, cherry-pick
fixes to `release/0.3.x`.

Create and merge a PR to bump the major or minor version on the `main` branch, using
`./release/bump_version_in_main.sh`. For example, if the current version is `0.5.0-dev`, bump it to
`0.6.0-dev` (minor) or `1.0.0-dev` (major); `main` always carries the `-dev` suffix.

On the release branch, bump the version to indicate pre-release by pushing a commit.

- If it is meant for internal testing, go with `alpha.1`, `alpha.2`, etc
- If it is meant for public testing, go with `beta.1`, `beta.2`, etc
- If it is ready for voting, go with `rc.1`, `rc.2`, etc

```shell
RELEASE_VER=x.y.z-rc.1

cargo set-version $RELEASE_VER --manifest-path crates/hudi/Cargo.toml
git commit -am "build(release): bump version to $RELEASE_VER"
```

### Testing

Once the "bump version" commit is pushed, the CI tests will be triggered and running.

> [!IMPORTANT]
> Perform any release-related tests in addition to the CI if applicable before proceeding to the next step.

### Make a local tag

Tag the revision locally for the RC. The tag should match to the version in the form of `release-*`. For example,

- If the release is `0.1.0-rc.1`, the tag should `release-0.1.0-rc.1`
- If the release is `2.0.0-beta.2`, the tag should `release-2.0.0-beta.2`

> [!NOTE]
> The local tag is required by the script for source release upload in the next step.
> Do not push the tag to the remote yet.

```shell
RELEASE_VER=x.y.z-rc.1

git tag release-$RELEASE_VER
```

### Upload source release to SVN dev

Pick the code-signing gpg key by running

```shell
gpg --list-secret-keys --keyid-format=long
```

> [!NOTE]
> Make sure you're using a computer that has your code-signing gpg key installed.

Run the below script to create and upload the source release artifacts.

```shell
RELEASE_VER=x.y.z-rc.1

./release/upload_src_release_to_dev.sh $RELEASE_VER ${YOUR CODE-SIGNING KEY ID}
```

Run the below script to verify the source release.

```shell
RELEASE_VER=x.y.z-rc.1

./release/verify_src_release.sh $RELEASE_VER dev
```

> [!IMPORTANT]
> Fix any issues if found before proceeding to the next step.

### Push tag

> [!CAUTION]
> Pushing a matching tag to the upstream (apache) branch will trigger CI to publish the artifacts to crates.io and
> pypi.org, which, if successful, is irreversible. Same versions are not allowed to publish more than once.

```shell
RELEASE_VER=x.y.z-rc.1

git push origin release-$RELEASE_VER
```

### Verify the published artifacts

Once the CI completes, check that every artifact the publish workflow builds actually reached
crates.io and pypi.org, and that the wheel works once installed.

```shell
RELEASE_VER=x.y.z-rc.1

./release/verify_published_artifacts.sh $RELEASE_VER
```

This checks all three crates on crates.io, the full wheel set plus the sdist on pypi.org, and then
installs the wheel in a clean container and reads a table with it. Pass `--skip-functional` to skip
the container step.

> [!IMPORTANT]
> A publish job can fail after its siblings have already uploaded, which leaves a version that looks
> published but is missing a platform. crates.io does not allow re-publishing a version, so a gap
> found here means starting a new release candidate, not repairing this one. Do not start the VOTE
> thread until this passes.

### Generate changelog

We use [cliff](https://git-cliff.org/) to generate changelogs. Install it by running the below command.

```shell
cargo install git-cliff
```

Switch to the release branch (org: apache/hudi-rs), generate a changelog for the release by running the below script.

```shell
PREV_RELEASE_VER=x.y.z

# for mac
git cliff release-$PREV_RELEASE_VER..HEAD | pbcopy

# for linux
git cliff release-$PREV_RELEASE_VER..HEAD | xclip
```

Paste the changelog output as a comment on the tracking issue.

### Start a `[VOTE]` thread

> [!NOTE]
> Update the template below in a text editor before pasting it to an email client to avoid hyperlink editing issue.

Send to `dev@hudi.apache.org`.

```text
[VOTE] hudi-rs x.y.z, release candidate #N

Hi everyone,

Please review and vote on hudi-rs x.y.z-rc.N as follows:

[ ] +1, Approve the release
[ ] -1, Do not approve the release (please provide specific comments)

The complete staging area is available for you to review:

* Release tracking issue is up-to-date [1]
* Categorized changelog for this release [2]
* Source release has been deployed to dist.apache.org [3]
* Source release can be verified using this script [4]
* Source code commit is tagged as "release-x.y.z-rc.N" [5]
* Source code commit CI has passed [6]
* Python artifacts have been published to pypi.org [7]
* Rust artifacts have been published to crates.io [8]

To verify basic functionality, install the RC and run the usage examples [9]:

    pip install --pre hudi==x.y.zrcN   # Python (PyPI normalizes "x.y.z-rc.N" to "x.y.zrcN")
    cargo add hudi@x.y.z-rc.N          # Rust

The vote will be open for at least 72 hours. It is adopted by majority
approval, with at least 3 PMC affirmative votes.

Thanks,
Release Manager

[1] https://github.com/apache/hudi-rs/issues/xxx
[2] https://github.com/apache/hudi-rs/issues/xxx#issuecomment-xxx
[3] https://dist.apache.org/repos/dist/dev/hudi/hudi-rs-x.y.z-rc.N/
[4] https://github.com/apache/hudi-rs/blob/<commit-hash>/release/verify_src_release.sh
[5] https://github.com/apache/hudi-rs/releases/tag/release-x.y.z-rc.N
[6] https://github.com/apache/hudi-rs/actions/runs/xxx
[7] https://pypi.org/project/hudi/x.y.zrcN/
[8] https://crates.io/crates/hudi/x.y.z-rc.N
[9] https://github.com/apache/hudi-rs?tab=readme-ov-file#usage-examples
```

## After VOTE passes

### Bump version in the release branch

Switch to the release branch (org: apache/hudi-rs), remove the pre-release suffix from the version.

```shell
RELEASE_VER=x.y.z

cargo set-version $RELEASE_VER --manifest-path crates/hudi/Cargo.toml
git commit -am "build(release): bump version to $RELEASE_VER"
git push
git tag release-$RELEASE_VER
```

> [!IMPORTANT]
> Wait for the CI to pass before proceeding to the next step.

### Upload source release to SVN release

Run the below script to create and upload the source release artifacts.

```shell
RELEASE_VER=x.y.z
CODE_SIGNING_KEY=<your code-signing key id>

./release/upload_src_release_to_dev.sh $RELEASE_VER $CODE_SIGNING_KEY

# re-uses the already-signed artifacts from the dev repo, so no signing key is needed
./release/publish_src_release.sh $RELEASE_VER
```

// TODO support verify local copy

Run the below script to verify the source release.

```shell
RELEASE_VER=x.y.z

./release/verify_src_release.sh $RELEASE_VER release
```

There shouldn't be any issue in verifying the source release at this step. But if any error came out from it, 
revert the last version bump commit, fix the reported error, and start a new release candidate.

### Push the release tag

> [!CAUTION]
> Pushing a matching tag to the upstream (apache) branch will trigger CI to publish the artifacts to crates.io and
> pypi.org, which, if successful, is irreversible. Same versions are not allowed to publish more than once.

```shell
RELEASE_VER=x.y.z

git push origin release-$RELEASE_VER
```

Once the CI completes, verify the published artifacts again for the official version.

```shell
RELEASE_VER=x.y.z

./release/verify_published_artifacts.sh $RELEASE_VER
```

### Update the change log

Switch to the release branch (org: `apache/hudi-rs`), and run the below script to copy the changelog for the release to the clipboard.

```shell
PREV_RELEASE_VER=x.y.z

# for mac
git cliff release-$PREV_RELEASE_VER..HEAD | pbcopy

# for linux
git cliff release-$PREV_RELEASE_VER..HEAD | xclip
```

In your fork, create a new branch from `main`, prepend the clipboard content to `CHANGELOG.md`, then commit and open a PR to merge into `main`.

```shell
git commit -am "chore: update changelog for x.y.z"
git push
```

### Publish Release Notes

Go to `https://github.com/apache/hudi-rs/releases/new` and draft a new release.

- Make the release title the same as the release version, e.g., `0.1.0`
- Paste the changelog content in the description
- Add a level-1 heading `# What's changed` at the top of the description, and remove redundant copied headings
- Close the tracking issue and the milestone

### Send `ANNOUNCE` email

> [!NOTE]
> Update the template below in a text editor before pasting it to an email client to avoid hyperlink editing issue.

Send to `dev@hudi.apache.org` and `user@hudi.apache.org`.

```text
subject: [ANNOUNCE] Hudi-rs 0.5.0 released

Hi all,

The Apache Hudi community is pleased to announce the release 0.5.0 of
Hudi-rs (https://github.com/apache/hudi-rs), the native Rust implementation for
Apache Hudi, with C++ and Python API bindings.

Highlights for this release:

- Highlight 1
- Highlight 2
- Highlight 3

The release notes can be found here
https://github.com/apache/hudi-rs/releases/tag/release-0.5.0

The source releases are available here
https://dist.apache.org/repos/dist/release/hudi/hudi-rs-0.5.0/

Please refer to the readme for installation and usage examples
https://github.com/apache/hudi-rs/blob/main/README.md

The Hudi community is active on these channels - we welcome you to engage 
with us!

- LinkedIn: https://www.linkedin.com/company/apache-hudi/
- X/Twitter: https://x.com/apachehudi/
- YouTube: https://www.youtube.com/@apachehudi
- Slack support: https://hudi.apache.org/slack

For users in China, follow WeChat "ApacheHudi" 微信公众号 for news and blogs,
and join DingTalk group 钉钉群 35087066 for questions.

Cheers,

{your name}, Release manager
```
