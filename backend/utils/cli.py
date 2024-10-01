import os
import click
import uvicorn
from logs.loggers.logger import logger_config
from utils.docker.util import is_docker

logger = logger_config(__name__)


@click.group()
def cli():
    """Main command group for os monitor."""
    pass


@cli.command()
@click.option(
    "--uvreload",
    default=not is_docker(),
    help="Checking if we are running in a docker or not",
)
def start(uvreload):
    """
    -- Run the FastAPI application
    """
    if uvreload:
        logger.warning(f"uvicorn reloading: {uvreload}")
    else:
        logger.warning(f"uvicorn not reloading b/c we are running in docker: {uvreload}")
    uvicorn.run("app.app:app", host="0.0.0.0", port=8000, reload=uvreload)

if __name__ == "__main__":
    cli()