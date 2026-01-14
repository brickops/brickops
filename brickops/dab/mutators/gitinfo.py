import os
import re


def _find_git_dir():
    """Recurse up the directory tree to find .git folder."""
    current_dir = os.path.abspath(os.getcwd())
    while True:
        git_dir = os.path.join(current_dir, ".git")
        if os.path.exists(git_dir) and os.path.isdir(git_dir):
            return git_dir
        parent_dir = os.path.dirname(current_dir)
        # Reached root directory
        if parent_dir == current_dir:
            return None
        current_dir = parent_dir


def get_git_info():
    """Get current git source url, branch, commit and tag without external libraries."""
    git_dir = _find_git_dir()
    if not git_dir:
        raise ValueError("No .git directory found in current or parent directories.")

    git_info = {}

    # Get current branch
    head_file = os.path.join(git_dir, "HEAD")
    if os.path.exists(head_file):
        with open(head_file, "r") as f:
            head_content = f.read().strip()

        if head_content.startswith("ref: refs/heads/"):
            # We're on a branch
            git_info["branch"] = head_content.replace("ref: refs/heads/", "")
            # Get commit hash from the branch ref
            branch_ref = os.path.join(git_dir, "refs", "heads", git_info["branch"])
            if os.path.exists(branch_ref):
                with open(branch_ref, "r") as f:
                    git_info["commit"] = f.read().strip()
        else:
            # Detached HEAD state - the content is the commit hash itself
            git_info["commit"] = head_content
            git_info["branch"] = "detached HEAD"

    # Get tag pointing to current commit
    current_commit = git_info.get("commit")
    if current_commit:
        tags_dir = os.path.join(git_dir, "refs", "tags")
        if os.path.exists(tags_dir):
            # Find all tags that point to the current commit
            matching_tags = []
            all_tags = []

            for tag_name in os.listdir(tags_dir):
                tag_file = os.path.join(tags_dir, tag_name)
                if os.path.isfile(tag_file):
                    with open(tag_file, "r") as f:
                        tag_commit = f.read().strip()

                    # Add to all tags list
                    all_tags.append(tag_name)

                    # Check if this tag points to current commit
                    if tag_commit == current_commit:
                        matching_tags.append(tag_name)

            if matching_tags:
                # Join multiple tags with comma and space
                git_info["tag"] = ", ".join(matching_tags)

            if all_tags:
                # Store all tags as comma-separated string
                git_info["all_tags"] = ", ".join(sorted(all_tags))

    # Get remote URL from config
    config_file = os.path.join(git_dir, "config")
    if os.path.exists(config_file):
        with open(config_file, "r") as f:
            config_content = f.read()

        # Look for origin remote URL
        match = re.search(
            r'\[remote "origin"\].*?url = (.+?)(?:\n|$)', config_content, re.DOTALL
        )
        if match:
            git_info["url"] = match.group(1).strip()

    # Store the repository root path
    git_info["repo_root"] = os.path.dirname(git_dir)

    return git_info
