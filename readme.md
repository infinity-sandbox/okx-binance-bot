# Binance Leaderboard Trading Bot

The bot is tested on Ubuntu only.

## Initial Setup
1. Clone this repository to your local machine.
2. Make sure you have MySQL installed on your system.
3. Update the `config.yml` file.

## Environment Setup
1. Create a virtual environment: `python3 -m venv venv`
2. Activate the virtual environment: `source venv/bin/activate`
3. Install required packages: `pip install -r requirements.txt`

## Database Setup
1. Run `python db_manager.py` once to set up the database.

## Running the Trading Bot (inside multiple TMUX sessions):
- To run a Telegram bot so you could check multiple scripts status (Tmux session 0): `python telegram_bot.py`
- To run a script that will constantly get top traders and their positions (Tmux session 1): `python rapidapi.py`
- To run a trading bot for top daily traders and x1 instance (Tmux session 2): `python leaderboad.py top_daily x1`
- To run a trading bot for top X traders and Y instance (Tmux session Z): `python leaderboad.py X Y` (where X: top_daily/top_weekly/top_monthly and Y: x1/x2/x3)

## Additional Notes
- To be updated.
