from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class GizmoSqlConnectionSettings(BaseModel):
    host: str
    port: int = 3306
    user: str
    password: SecretStr
    use_tls: bool = False
    extra_db_params: dict | None = None
    conn_kwargs: dict | None = None

    @property
    def uri(self):
        scheme = "grpc+tls" if self.use_tls else "grpc"
        return f"{scheme}://{self.host}:{self.port}"

    @property
    def db_kwargs(self):
        params = self.extra_db_params or {}
        credentials = {
            "username": self.user,
            "password": self.password.get_secret_value(),
        }
        return {**credentials, **params}


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
