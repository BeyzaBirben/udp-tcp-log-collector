import os
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class APISettings:
    host: str = "0.0.0.0"
    port: int = 8000
    default_page_size: int = 50
    max_page_size: int = 200

    def __post_init__(self) -> None:
        if not isinstance(self.host, str) or not self.host.strip():
            raise ValueError("API_HOST must not be empty")

        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("API_PORT must be between 1 and 65535")

        page_sizes = {
            "API_DEFAULT_PAGE_SIZE": self.default_page_size,
            "API_MAX_PAGE_SIZE": self.max_page_size,
        }

        for variable_name, value in page_sizes.items():
            if type(value) is not int or value <= 0:
                raise ValueError(f"{variable_name} must be a positive integer")

        if self.default_page_size > self.max_page_size:
            raise ValueError(
                "API_DEFAULT_PAGE_SIZE must not exceed API_MAX_PAGE_SIZE"
            )

    @classmethod
    def from_environment(
        cls,
        values: Mapping[str, str] | None = None,
    ) -> "APISettings":
        environment = os.environ if values is None else values

        return cls(
            host=environment.get("API_HOST", "0.0.0.0").strip(),
            port=_integer(environment, "API_PORT", 8000),
            default_page_size=_integer(
                environment,
                "API_DEFAULT_PAGE_SIZE",
                50,
            ),
            max_page_size=_integer(
                environment,
                "API_MAX_PAGE_SIZE",
                200,
            ),
        )


def _integer(
    environment: Mapping[str, str],
    variable_name: str,
    default: int,
) -> int:
    raw_value = environment.get(variable_name, str(default))

    try:
        return int(raw_value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{variable_name} must be an integer") from error
