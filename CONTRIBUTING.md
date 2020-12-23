# Contributing

Check out ways to contribute to this project...

## Feature requests

When you have an idea on how we could improve, please check our 
[discussions](https://github.com/ing-bank/cassandra-jdbc-wrapper/discussions) to see if there are similar ideas or 
feature requests. If there are none, please [start](https://github.com/ing-bank/cassandra-jdbc-wrapper/discussions/new) 
your feature request as a new discussion topic. Add the title `[Feature Request] My awesome feature` and a description 
of what you expect from the improvement and what the use case is.

## Report a bug

If you think you have found a bug, first make sure that you are testing against the latest version of the project. If 
not, maybe your issue has already been fixed in a more recent version.
Otherwise, search [our issues list on GitHub](https://github.com/ing-bank/cassandra-jdbc-wrapper/issues) if a similar 
issue has not already been opened. If you didn't find an existing similar issue, feel free to
[create a new one](https://help.github.com/en/github/managing-your-work-on-github/creating-an-issue) respecting the
following recommendations:

* Provide a quick summary of the problem.
* Prepare a reproduction of the bug by preparing a simple test case we can run to reproduce your bug. It will be helpful
to find and fix the problem.
* Describe the result you expected.
* Don't hesitate to provide as much information as you can. Often, the devil is in the detail!

## Contributing code: we ♥ pull requests

Help out the whole community by sending your pull requests. Check out how to set it up:

### Fork and clone the repository

To contribute to this project, you will need to fork its repository and clone it to your local machine.
See [GitHub help page](https://help.github.com/articles/fork-a-repo) for help.

The command to type in order to clone the forked repository to your local machine should be:
```
git clone https://github.com/{your username}/cassandra-jdbc-wrapper.git
```

### Configure the project and run tests

Once you cloned the repository to your local machine, open the project in your favorite IDE.

To build the project, execute the following command:
```
mvn clean install
```

To run the tests, execute the following command:
```
mvn test
```

### Submit a pull request

Once your changes and tests are ready for review, submit them:

1. Be sure that your changes are tested.

2. Check your code is documented enough.

3. Rebase your changes: update your local repository with the most recent code from the original repository, and rebase
your branch on top of the latest `release/next` branch. It is better that your initial changes are squashed into a
single commit. If more changes are required to validate the pull request, we invite you to add them as separate commits.

4. Finally, push your local changes to your forked repository and submit a pull request into the branch `release/next`
with a title which sums up the changes that you have made (try to not exceed 50 characters), and provide more details in
the body. If necessary, also mention the number of the issue solved by your changes, e.g. "Closes #123".

### License headers

This project is distributed under Apache License 2.0. The license headers are required on all the Java files. So, all
contributed code should have the following license header:
```
/*
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
```
