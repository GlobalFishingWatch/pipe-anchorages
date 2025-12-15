# GitHub Flow

[GitHub Flow]: https://githubflow.github.io/

<div align="justify">

[GitHub Flow] is an ideal workflow for teams that deploy frequently
and want to iterate quickly without the overhead of long release cycles or complex branching strategies.
It keeps the process simple and fast,
enabling continuous integration and deployment,
which helps catch issues early and deliver features to users rapidly.

These are the basic rules of the workflow:
1. **`main` is always deployable**

  The `main` branch must be stable and ready to deploy at all times. Never push untested or broken code.

2. **Create descriptively named branches from `main`**

  Start every new feature, fix, or idea in a new branch based on `main`, with clear and meaningful names.

3. **Push to your branch often**

  Push work regularly to the remote to back it up, share progress, and trigger CI.

4. **Open a Pull Request early**

  Use pull requests for code review, discussion, or help—long before the branch is “done.”

5. **Get reviewed and approved before merging**

  Don’t merge until someone reviews and approves your work (even with just a 👍 or `:shipit:`).

6. **Deploy right after merging to `main`**

  Once merged, deploy immediately or soon—everything in `main` should be safe and shippable.

---

This flow emphasizes **simplicity, speed, and collaboration**, especially for teams that deploy frequently.

### Additional team conventions

- If you plan to make a release immediately after merging,
  bump the version directly in your feature branch.
  This avoids the need to create a separate release branch just for the version bump.

- Always rebase feature branches on top of `main` before merging them.
  This, combined with enforcing **merge commits** in GitHub repository settings,
  results in a semi-linear history on the `main` branch,
  which is very helpful when using `git log`.[^1]
  If **merge commits** are not enforced,
  Git will perform a **fast-forward merge** after rebasing,
  creating a linear history
  and losing the context of the feature branch.
  If merging locally, avoid fast-forward merges with:
  ```shell
  git checkout main
  git pull
  git merge branch_name --no-ff
  ```

[^1]: When performing a rebase, it's acceptable to `push --force` to your own feature branch,
      but **never** on shared branches.
      Protect `main` and/or `develop` from force pushes via GitHub settings.  

</div>
