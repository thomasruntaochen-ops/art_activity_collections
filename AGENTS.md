## Environment
Before running any Python script, test, or tool that depends on local Python packages or shell-managed environment variables, run it in the same shell command after sourcing `~/.zshrc`.

Examples:
- `source ~/.zshrc && python script.py`
- `source ~/.zshrc && pytest`
- `source ~/.zshrc && python -m package.module`

`source ~/.zshrc` must be in the same command or shell session as the Python invocation. It does not persist across separate command executions.
