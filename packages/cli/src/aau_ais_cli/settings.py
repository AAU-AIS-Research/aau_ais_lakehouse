from functools import cache

from aau_ais_core.settings import GizmoSqlConnectionSettings
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class TrajectorySettings(BaseModel):
    min_speed_threshold: float = 1  # In knots
    max_speed_threshold: float = 100  # In knots
    temporal_threshold: int = Field(default=600, gt=180)
    spatial_threshold: int = 4000  # In meters
    start_trajectory_window_size: int = 3
    stop_trajectory_window_size: int = 20


class Settings(BaseSettings):
    gizmosql: GizmoSqlConnectionSettings
    trajectory: TrajectorySettings = TrajectorySettings()

    model_config = SettingsConfigDict(
        extra="ignore",
        env_file=".env",
        case_sensitive=False,
        env_nested_delimiter="__",
    )

    @staticmethod
    @cache
    def create() -> "Settings":
        return Settings()  # type: ignore
