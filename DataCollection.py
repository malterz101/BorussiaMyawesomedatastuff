import re
import json
import requests
import pandas as pd
import psycopg2
from datetime import datetime


class Bundesliga:

    """
        Data collection for the Bundesliga front-end, this collects and transforms publicly available data
        relating to football in Germany.
    """

    def open_database(self):

        conn = psycopg2.connect(dbname="bundesdata", user="will", password="amazing hire")
        conn.autocommit = True
        return conn.cursor()

    def create_tables(self):

        """ create tables in the PostgreSQL database"""

        game_data = """
            CREATE TABLE game_data (
                date DATE NOT NULL,
                score VARCHAR(255) NOT NULL,
                homeTeam VARCHAR(255) NOT NULL,
                awayTeam VARCHAR(255) NOT NULL
            );
        """

        season_data = """ 
            CREATE TABLE season_data (
                date DATE NOT NULL,
                points INT NOT NULL,
                club VARCHAR(255) NOT NULL,
                scored INT NOT NULL,
                conceded INT NOT NULL
            );
        """

        league_data = """ 
            CREATE TABLE league_data (
                mini_icon VARCHAR(255) NOT NULL,
                club VARCHAR(255) NOT NULL,
                average_player_value FLOAT NOT NULL,
                total_market_value FLOAT NOT NULL
            );
        """

        player_data = """ 
            CREATE TABLE player_data (
                mini_icon VARCHAR(255) NOT NULL,
                club VARCHAR(255) NOT NULL,
                player_name VARCHAR(255) NOT NULL,
                market_value FLOAT NOT NULL
            );
        """
        cursor = self.open_database()
        try:
            cursor.execute(player_data)
        except Exception as e:
            print(e)

        try:
            cursor.execute(league_data)
        except Exception as e:
            print(e)

        try:
            cursor.execute(season_data)
        except Exception as e:
            print(e)

        try:
            cursor.execute(game_data)
        except Exception as e:
            print(e)


    def transfermarkt_scraper(self, year=2021):
        """
            League: https://www.transfermarkt.co.uk/1-bundesliga/startseite/wettbewerb/L1
            Players: https://www.transfermarkt.co.uk/1-bundesliga/marktwerte/wettbewerb/L1
            Table: https://www.transfermarkt.co.uk/bundesliga/tabelle/wettbewerb/L1/saison_id/2021
            Result Grid: https://www.transfermarkt.co.uk/bundesliga/kreuztabelle/wettbewerb/L1/saison_id/2021
            Remaining Matches: https://www.transfermarkt.co.uk/bundesliga/restprogramm/wettbewerb/L1/saison_id/2021

            :return: JSON data with images/data/market values
        """


        # Headers to make me look a bit less like a machine

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36 OPR/83.0.4254.62",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"
        }

        league = requests.get(
            'https://www.transfermarkt.co.uk/1-bundesliga/startseite/wettbewerb/L1',
            headers=headers
        ).content

        cursor = self.open_database()

        league_data = []

        info = re.compile("class=\"(?:odd|even)\"(.*?)</tr>", re.DOTALL)
        for row in re.findall(info, str(league)):
            try:
                icon = re.compile('zentriert.*?src=.(.*?)\"')
                mini_icon = re.findall(icon, row)[0]

                name = re.compile('zentriert.*?alt=.(.*?)\"')
                club = re.findall(name, row)[0]
                print(row)
                player_value = re.compile('rechts\">.xc2.xa3(.*?[a-z])</td>')
                avg_player_value = re.findall(player_value, row)[0]

                if avg_player_value[-1] == "m":
                    avg_player_value = float(avg_player_value[:-1])*1000000
                else:
                    avg_player_value = float(avg_player_value[:-1]) * 100000000

                market_value = re.compile(str(year)+'\">.xc2.xa3(.*?[a-z]*?)</a>')
                total_squad_value = re.findall(market_value, row)[0]

                if total_squad_value[-1] == "m":
                    total_squad_value = float(total_squad_value[:-1])*1000000
                else:
                    total_squad_value = float(total_squad_value[:-1]) * 100000000

                # avg_player_value = market_values[0]
                # total_squad_value = market_value[1]

                cursor.execute(
                    """
                        INSERT INTO league_data (
                            mini_icon, club, average_player_value, total_market_value 
                        ) VALUES (%s, %s, %s, %s);
                    """, (
                        mini_icon,
                        club,
                        avg_player_value,
                        total_squad_value
                    )
                )
            except Exception as e:
                print(row)
        players = requests.get(
            'https://www.transfermarkt.co.uk/1-bundesliga/marktwerte/wettbewerb/L1',
            headers=headers
        ).content

        info = re.compile("class=\"(?:odd|even)\"(.*?)</tr>", re.DOTALL)
        for row in re.findall(info, str(players)):

            try:

                image = re.compile(".*?src=.(.*?)\"", re.DOTALL)
                player_image = re.findall(image, row)[0]

                name = re.compile("hauptlink.*?title=.(.*?)\"", re.DOTALL)
                player = re.findall(name, row)[0]

                club = re.compile("zentriert.*?title=.(.*?)\"", re.DOTALL)
                club_name = re.findall(club, row)[0]

                market_value = re.compile('\">.xc2.xa3(.*?[a-z])</a>')
                market_value = re.findall(market_value, row)[0]

                if market_value[-1] == "m":
                    market_value = float(market_value[:-1])*1000000
                else:
                    market_value = float(market_value[:-1]) * 100000000

                cursor.execute(
                    """
                        INSERT INTO player_data (
                            mini_icon, club, player_name, market_value 
                        ) VALUES (%s, %s, %s, %s);
                    """, (
                        player_image,
                        club_name,
                        player,
                        market_value
                    )
                )
            except Exception as e:
                print(row)

        return "Complete"

    def football_api_data(self, league_id=2002, backFill=False):

        """
        Football data, Bundesliga is on ID 2002
        :return:
        """

        url = "http://api.football-data.org/v2/competitions/{}/matches".format(league_id)
        if not backFill:
            url += "/?dateFrom={}".format(datetime.today().strftime('%Y-%m-%d'))

        data = requests.get(
            url,
            headers={
                "X-Auth-Token": "3ec15a4b470e4555a40a3f4286f43075"
            }
        ).json()

        cursor = self.open_database()

        for x in data["matches"]:

            cursor.execute(
                """
                    INSERT INTO game_data (
                        date, score, homeTeam, awayTeam 
                    ) VALUES (%s, %s, %s, %s);
                """, (
                    x['utcDate'],
                    str(x['score']['fullTime']['homeTeam']) + " - " + str(x['score']['fullTime']['awayTeam']),
                    x['homeTeam']['name'],
                    x['awayTeam']['name']
                )
            )

            if x['score']['fullTime']['homeTeam']:
                if x['score']['fullTime']['homeTeam'] - x['score']['fullTime']['awayTeam'] == 0:

                    cursor.execute(
                        """
                            INSERT INTO season_data (
                                date, club, points, scored, conceded 
                            ) VALUES (%s, %s, %s, %s, %s);
                        """, (
                            x['utcDate'],
                            x['homeTeam']['name'],
                            1,
                            x['score']['fullTime']['homeTeam'],
                            x['score']['fullTime']['awayTeam']
                        )
                    )


                    cursor.execute(
                        """
                            INSERT INTO season_data (
                                date, club, points, scored, conceded 
                            ) VALUES (%s, %s, %s, %s, %s);
                        """, (
                            x['utcDate'],
                            x['awayTeam']['name'],
                            1,
                            x['score']['fullTime']['awayTeam'],
                            x['score']['fullTime']['homeTeam']
                        )
                    )

                elif x['score']['fullTime']['homeTeam'] - x['score']['fullTime']['awayTeam'] > 0:
                    cursor.execute(
                        """
                            INSERT INTO season_data (
                                date, club, points, scored, conceded 
                            ) VALUES (%s, %s, %s, %s, %s);
                        """, (
                            x['utcDate'],
                            x['homeTeam']['name'],
                            3,
                            x['score']['fullTime']['awayTeam'],
                            x['score']['fullTime']['homeTeam']
                        )
                    )
                elif x['score']['fullTime']['homeTeam'] - x['score']['fullTime']['awayTeam'] < 0:
                    cursor.execute(
                        """
                            INSERT INTO season_data (
                                date, club, points, scored, conceded 
                            ) VALUES (%s, %s, %s, %s, %s);
                        """, (
                            x['utcDate'],
                            x['awayTeam']['name'],
                            3,
                            x['score']['fullTime']['homeTeam'],
                            x['score']['fullTime']['awayTeam']
                        )
                    )

        return "Tables Updates"

    def live_odds_api(self, sport):
        """
        https://the-odds-api.com/liveapi/guides/v4/#endpoint

        :param sport:
        :return:
        """

        odds = []

        return odds

Bundesliga().transfermarkt_scraper()
# Bundesliga().football_api_data(2002, False)