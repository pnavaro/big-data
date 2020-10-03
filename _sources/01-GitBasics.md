---
jupytext:
  cell_metadata_json: true
  encoding: '# -*- coding: utf-8 -*-'
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: '0.9'
    jupytext_version: 1.5.2
kernelspec:
  display_name: big-data
  language: python
  name: big-data
---

+++ {"slideshow": {"slide_type": "slide"}}

# Git basics

## About Dropbox

- Dropbox versioning is not free.
- Only keep your edits over a period of 30 days.
- Privacy and Security ?
- No differences display.
- The service have the right to delete information from free and inactive accounts.
- Users are not allowed to perform encryption.

+++ {"slideshow": {"slide_type": "slide"}}

## About version control

- Records changes to a file or set of files over time.
- You can recall specific versions later.
- You can use it with nearly any type of file on a computer.
- This is the better way to collaborate on the same document.

+++ {"slideshow": {"slide_type": "slide"}}

## Centralized Version Control Systems

<img src="images/cvs.png" width=400>

- Clients check out files from a central place.
- You know what everyone else on the project is doing
- A single server contains all the versioned files.
- For many years, this has been the standard (CVS, SVN).
- You always need network connection.
- If the server is corrupted, with no backup, you could lose everything !

+++ {"slideshow": {"slide_type": "slide"}}

## Git

Git is a free and open source distributed version control system designed to handle everything from small to very large projects with speed and efficiency.

Official website https://git-scm.com

**New products based on a git server for collaborating writing.**

- ShareLaTeX (https://fr.sharelatex.com)
- Authorea (https://www.authorea.com)
- Overleaf (https://www.overleaf.com)
- PLMLateX (https://plmlatex.math.cnrs.fr/)

+++ {"slideshow": {"slide_type": "slide"}}

## GitHub

- Web-based hosting service for version control using Git. 
- Offers all of the distributed version control and source code management (SCM) functionality of Git as well as adding its own features. 
- Provides access control and several collaboration features such as bug tracking, feature requests, task management, and wikis for every project.
- GitHub is the largest host of source code in the world.
- GitHub evolves towards a social network and offers a better visibility to your work.
- Julia language depends heavily on GitHub. Almost all R and Python packages developers use this platform.

[Gitlab.com](https://about.gitlab.com) and [Bitbucket](https://bitbucket.org) offer similar services.

+++ {"slideshow": {"slide_type": "slide"}}

## Distributed Version Control Systems

<img src="images/git.png" alt="git" width="300px"/>

- Clients fully mirror the repository.
- You can collaborate with different groups of people in different ways simultaneously within the same project.
- No need of network connection.
- Multiple backups.

+++ {"slideshow": {"slide_type": "slide"}}

## Git bash

I you want to try Git on windows, install [git bash](https://gitforwindows.org). On Linux and Mac just open a terminal.

![git bash](https://gitforwindows.org/img/gw1web_thumb.png)

+++ {"slideshow": {"slide_type": "slide"}}

## Configure Git

Settings are saved on the computer for all your git repositories.

```bash
$ git config --global user.name "Prenom Nom"
$ git config --global user.email "prenom.nom@univ-rennes2.fr"
$ git config --list
```
---
~~~
user.name=Prenom Nom
user.email=prenom.nom@univ-rennes2.fr
~~~

+++ {"slideshow": {"slide_type": "slide"}}

## Initialize a git repository in a directory

Create the directory `homepage`:

```bash
$ mkdir homepage                          # Create directory homepage
$ cd homepage                             # Change directory
$ touch index.md                          # Create the index.md file
$ echo "# John Smith " >> index.md        # Write the string "# Test" in index.md
$ echo "Rennes" >> index.md               # Append Rennes to index.md
$ cat index.md                            # Display index.md content
# John Smith
Rennes
```

+++ {"slideshow": {"slide_type": "slide"}}

To use git in this repository
```bash
$ git init
Initialized empty Git repository in /Users/navaro/homepage/.git/
$ git status
On branch master

No commits yet

Untracked files:
  (use "git add <file>..." to include in what will be committed)
	index.md

nothing added to commit but untracked files present (use "git add" to track)
```

+++ {"slideshow": {"slide_type": "slide"}}

### Add the file to the git index

```bash
$ git add index.md
$ git status
On branch master

No commits yet

Changes to be committed:
  (use "git rm --cached <file>..." to unstage)
	new file:   index.md
```

+++ {"slideshow": {"slide_type": "slide"}}

## Commit

```bash
$ git commit -m 'Create the file index.md'
[master (root-commit) 63a5cee] Create the file index.md
 1 file changed, 2 insertions(+)
 create mode 100644 index.md
```
 
```bash
$ git status
On branch master
nothing to commit, working tree clean
```

+++ {"slideshow": {"slide_type": "slide"}}

## Four File status in the repository
  
<img src="images/18333fig0201-tn.png" alt="git" width="450px"/>

+++ {"slideshow": {"slide_type": "slide"}}

## Github account and SSH key

[Create your GitHub account](https://github.com/join)

### Generating public/private rsa key pair.

Ref : [New ssh key on GitHub](https://docs.github.com/en/github/authenticating-to-github/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent)

Open Terminal or Git bash.

Paste the text below, substituting in your GitHub email address.
```bash
ssh-keygen -t rsa -b 4096 -C "prenom.nom@univ-rennes2.fr" -N ""
```
This creates a new ssh key, using the provided email as a label.

+++ {"slideshow": {"slide_type": "slide"}}

When you're prompted to "Enter a file in which to save the key," press Enter. This accepts the default file location. Enter a file in which to save the key (/Users/you/.ssh/id_rsa): [Press enter]
At the prompt, let it empty for no passphrase.

This creates 2 files, the private key: `id_rsa`, and the public key `id_rsa.pub`. Copy the SSH key to your clipboard.

```bash
pbcopy < ~/.ssh/id_rsa.pub
```

In the upper-right corner of any page, click your profile photo, then click `Settings`.
In the user settings sidebar, click `SSH and GPG keys`.  Click `New SSH key` or `Add SSH key` and paste the key.

+++ {"slideshow": {"slide_type": "slide"}}

## GitHub repository

<font color="red">In the following steps replace **your_login** by your own GitHub login</font>

Create the `your_login.github.io` repository on your GitHub account
- Click on '+' on top right of the page and select "New repository"
- Repository name = "your_login.github.io"
- Don't change default options
- Click on "Create repository"

```bash
$ cd homepage
$ git remote add origin git@github.com:your_login/your_login.github.io.git
$ git push -u origin master
Enumerating objects: 6, done.
Counting objects: 100% (6/6), done.
Delta compression using up to 4 threads
Compressing objects: 100% (2/2), done.
Writing objects: 100% (4/4), 449 bytes | 449.00 KiB/s, done.
Total 4 (delta 0), reused 0 (delta 0), pack-reused 0
To github.com:your_login/your_login.github.io.git
   fd6dace..c4488e6  master -> master
```

```bash
$ git status
On branch master
nothing to commit, working tree clean
```

+++ {"slideshow": {"slide_type": "slide"}}

## Enable GitHub pages

Go to <https://github.com/your_login.github.io/settings>

In the section **GitHub Pages**

Select `master` for **Source** and `root`

Choose the **minimal theme** and validate, you can change it later.

The website is available at <https://your_login.github.io>

+++ {"slideshow": {"slide_type": "slide"}}

## Git Workflow

<img src="images/four_stages.png" alt="git" width="150px"/>

+++

By choosing a theme, you create on GitHub a file named "_config.yml". You need to update your local version with

```bash
git pull --no-edit
```
The `--no-edit` function avoid spawning a text editor, and asking for a merge commit message.
If you do
```bash
ls
```
You will display the new file _config.yml

+++

## Exercise

Check the web page by visiting <https://your_login.github.io>

Modify the file index.md and do the procedure again. Modify also the file `_config.yml` by appending the following content:

```yaml
title: Page personnelle
description: Exercice Git
```

+++ {"slideshow": {"slide_type": "slide"}}

## Branches

**Display all branches**

```bash
$ git branch -a
* master
  remotes/origin/master
```

+++ {"slideshow": {"slide_type": "slide"}}

## Create a new branch

By creating a new branch you freeze the master branch and you can continue to work without modifying it.
The branch created is the copy of the current branch (master).

```bash
$ git branch mybranch
$ git checkout mybranch
Switched to branch 'mybranch'
```

```bash
$ git branch
master
* mybranch
```

Files could be different or not existing in two branches but they are located at the same place on the file system. When you use the `checkout` command, git applies the changes.

+++ {"slideshow": {"slide_type": "slide"}}

## Edit and modify the index.md file

```bash
$ echo '![logo](https://intranet.univ-rennes2.fr/sites/default/files/resize/UHB/SERVICE-COMMUNICATION/logor2-noir-150x147.png)' >> index.md
$ git status
On branch mybranch
Changes not staged for commit:
  (use "git add <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
	modified:   index.md

no changes added to commit (use "git add" and/or "git commit -a")
```

```bash
$ git diff
diff --git a/index.md b/index.md
index 87dde03..af6739c 100644
--- a/index.md
+++ b/index.md
@@ -3,3 +3,4 @@

+![logo](https://intranet.univ-rennes2.fr/sites/default/files/resize/UHB/SERVICE-COMMUNICATION/logor2-noir-150x147.png)
```

+++ {"slideshow": {"slide_type": "slide"}}

## Commit the changes

```bash
$ git add index.md
$ git status
On branch mybranch
Changes to be committed:
  (use "git restore --staged <file>..." to unstage)
	modified:   index.md
```

```bash
$ git commit -m 'Add logo'
[mybranch 30a8912] Add logo
 1 file changed, 1 insertion(+)
```

+++ {"slideshow": {"slide_type": "slide"}}

## Commit or fast commit

```{image} images/index1.png
:width: 400px
```
```{image} images/index2.png
:width: 400px
```

+++ {"slideshow": {"slide_type": "slide"}}

## Merge mybranch with the master branch


```bash
$ git diff master
diff --git a/index.md b/index.md
index c744020..4d833d1 100644
--- a/index.md
+++ b/index.md
@@ -1,2 +1,3 @@
 # Prenom Nom
 Rennes
+![logo](https://intranet.univ-rennes2.fr/sites/default/files/resize/UHB/SERVICE-COMMUNICATION/logor2-noir-150x147.png)
```

+++ {"slideshow": {"slide_type": "slide"}}

## Push master branch

```bash
$ git checkout master
Switched to branch 'master'
$ git merge mybranch
Updating 63a5cee..30a8912
Fast-forward
 index.md | 1 +
 1 file changed, 1 insertion(+)
$ git push origin master
Enumerating objects: 9, done.
Counting objects: 100% (9/9), done.
Delta compression using up to 4 threads
Compressing objects: 100% (5/5), done.
Writing objects: 100% (8/8), 869 bytes | 869.00 KiB/s, done.
Total 8 (delta 1), reused 0 (delta 0), pack-reused 0
remote: Resolving deltas: 100% (1/1), completed with 1 local object.
To github.com:your_login/your_login.github.io.git
   6dafcbd..340d3dc  master -> master
```

+++ {"slideshow": {"slide_type": "slide"}}

## Collaborating writing with a git repository

![Cycle](images/git_cycle.png)

+++ {"slideshow": {"slide_type": "slide"}}

## Clone the remote repository

```bash
git clone git@github.com:MMASSD/atelier_git.git
```

+++ {"slideshow": {"slide_type": "slide"}}

## Share your modifications

### Option 1 : merge master branch and push it

```bash
$ git checkout master
$ git merge mybranch
$ git push origin master
```

### Option 2 : Push your branch 

```bash
$ git checkout mybranch
$ git push origin mybranch
```

+++ {"attributes": {"classes": ["git"], "id": ""}, "slideshow": {"slide_type": "slide"}}

## Synchronize the repository

### Update master branch

```bash
$ git checkout master
```

You can use `pull` which is a `fetch and merge` command

```bash
$ git pull origin master
```
`origin` is the repository and `master` the **remote** branch from you want to update.

+++ {"attributes": {"classes": ["git"], "id": ""}, "slideshow": {"slide_type": "slide"}}

In some cases, it could be safer to check the differences between your local and the remote branch with

```bash
$ git fetch origin
$ git diff origin/master
```
and merge
```bash
$ git merge origin master
```

### Update personal branch

```bash
$ git checkout mybranch
$ git merge master
```

+++ {"slideshow": {"slide_type": "slide"}}

## Rebase

If the branch named `mybranch` is local and never pushed to the repository. It is possible to use **rebase** instead of merge. `git rebase` reapply commits on top of another base tip.

+++ {"slideshow": {"slide_type": "slide"}}

Exercise:

- Create a new branch called `test_rebase` from master.
- Do some modifications on the remote master branch by editing file in your browser on GitHub.
- On your `test_rebase` do also some modifications. Type
```bash 
$ git log -n 2
```
It displays the last two commits
- Switch and update your master branch
- Switch back to `test_rebase` and rebase the local branch with:
```bash
$ git rebase master
```
- Displays the last two commits and check how the history was changed

+++ {"slideshow": {"slide_type": "slide"}}

## Stash

Use git stash when you want to record the current state of the working directory and the index, but want to go back to a clean working directory. The command saves your local modifications away and reverts the working directory to match the HEAD commit.

+++ {"slideshow": {"slide_type": "slide"}}

```bash
$ date >> index.md    # Modify the index.md file

$ git diff
diff --git a/index.md b/index.md
index 4d833d1..a7bc91d 100644
--- a/index.md
+++ b/index.md
@@ -1,3 +1,4 @@
 # Prenom Nom
 Rennes
 ![logo](https://intranet.univ-rennes2.fr/sites/default/files/resize/UHB/SERVICE-COMMUNICATION/logor2-noir-150x147.png)
+Sun Sep 13 21:45:41 CEST 2020

$ git stash
Saved working directory and index state WIP on master: 0fc9d7d Merge remote-tracking branch 'origin/master' into master

$ git stash show
 index.md | 1 +
 1 file changed, 1 insertion(+)
 
$ git status
On branch master
nothing to commit, working tree clean
```

+++ {"slideshow": {"slide_type": "slide"}}

```bash
$ git stash pop
On branch master
Changes not staged for commit:
  (use "git add <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
	modified:   index.md

no changes added to commit (use "git add" and/or "git commit -a")
Dropped refs/stash@{0} (3de29586d5e0b9ceeb3c23e03a9aeb045c4096b8)

$ git status
On branch master
Changes not staged for commit:
  (use "git add <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
	modified:   index.md

no changes added to commit (use "git add" and/or "git commit -a")
```

+++ {"slideshow": {"slide_type": "slide"}}

### Apply last `stash`

```bash
$ git stash apply
```

### Delete the last `stash`

```bash
$ git stash drop
```

### Apply last `stash` and drop

```bash
$ git stash pop
```

+++ {"slideshow": {"slide_type": "slide"}}

## Merge conflicts

If you have conflict, try :

```bash
$ git mergetool
```

A nice editor helps you to choose the right version. Close and :

```bash
$ git commit -m 'Update and fixed conflicts'
```

+++ {"slideshow": {"slide_type": "slide"}}

## Why Git?

- Tracking and controlling changes in the software.
- Branches : Frictionless Context Switching, Role-Based Codelines.
- Everything is local : Git is fast.
- Multiple Backups.
- It's impossible to get anything out of Git other than the exact bits you put in.
- Staging Area : intermediate index between working directory and repository.
- Pull-request is a nice feature for code reviewing and protect the stable branch.

+++ {"slideshow": {"slide_type": "slide"}}

## Why not

- Sometimes confusing for new users coming from CVS or subversion.
- Crazy command line syntax.
- Simple tasks need many commands.
- Git history is often strange.
- It is possible to destroy the repository on the remote server.
- Power for the maintainer, at the expense of the contributor.

+++ {"slideshow": {"slide_type": "slide"}}

## Some useful commands

- Showing which files have changed between git branches

```
$ git diff --name-status master..mybranch
```
- Compare the master version of a file to my current branch version

```
$ git diff mybranch master -- myfile
```

- Remove all ignored files (do it after a commit)

```
$ git clean -xdf
```

+++ {"slideshow": {"slide_type": "slide"}}

## Revert the last commit

```bash
$ date >> index.md ## modify index.md

$ git commit -a -m 'Add date in index.md'
[master cbfb502] Add date in index.md
 3 files changed, 18 insertions(+)
 create mode 100644 .gitignore
 create mode 100644 sandbox.Rproj

$ git reset HEAD~1
Unstaged changes after reset:
M	index.md

$ git diff
diff --git a/index.md b/index.md
index c744020..5d9bd58 100644
--- a/index.md
+++ b/index.md
@@ -1,2 +1,3 @@
 # Prenom Nom
 Rennes
+Sun Sep 13 22:17:05 CEST 2020

$ git checkout index.md
Updated 1 path from the index

$ cat index.md
# Prenom Nom
Rennes
```

+++ {"slideshow": {"slide_type": "slide"}}

## Git through IDE

- Install bash-completion and source git-prompt.sh.
- Use Gui tools:
	- [GitHub Desktop](https://desktop.github.com/)
	- [Sourcetree](https://fr.atlassian.com/software/sourcetree)
	- [GitKraken](https://www.gitkraken.com/)
- VCS plugin of IDE
	- [RStudio](https://www.rstudio.com/)
	- [Eclipse](https://www.eclipse.org/downloads/)
	- [TeXstudio](www.texstudio.org/)
	- [JetBrains](https://www.jetbrains.com/)
