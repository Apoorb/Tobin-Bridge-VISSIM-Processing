from pathlib import Path
import inflection


def get_project_root() -> Path:
    return Path(__file__).parent.parent


def remove_special_char_vissim_col(df_columns):
    return [
        inflection.underscore(colnm)
        .replace("$", "")
        .replace(":", "_")
        .replace("\\", "_")
        .replace("(", "_")
        .replace(")", "")
        .replace(".", "")
        .strip()
        for colnm in df_columns
    ]
