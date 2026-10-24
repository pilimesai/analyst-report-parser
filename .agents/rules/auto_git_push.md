# Auto Git Push Rule

## Core Directive
Whenever any code, configuration, or data files in this repository (`report-analyzer`) are modified, added, or deleted as part of completing a user request or task, the agent **MUST automatically commit and push** the changes to GitHub (`origin main`).

## Workflow Steps
1. Before committing, check `git status` to verify modified and untracked files.
2. If remote changes exist, pull with rebase (`git pull --rebase origin main`).
3. Commit with a clear and descriptive commit message in Traditional Chinese or Conventional Commits style (e.g., `feat: ...`, `fix: ...`, `chore: ...`).
4. Push to remote (`git push origin main`).
5. Confirm push success before completing the response to the user.
