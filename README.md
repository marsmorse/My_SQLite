# my-sqlite


## Summary ##
my_sqlite is simple version of clone of sqlite that supports the following operations on .csv files on a commanda sqlite program that interfaces with csv data files through the command line.
 It Supports
## Run Instructions ##
Clone the git repo

import any csv files that you would like to use as data files
Run my_sqlite via the command line in the root folder
```
ruby my_sqlite_cli.rb
```
start inputing sql commands with your csv file name (including the .csv) inplace of a table name!
NOTE all commands must be followed by a ;
## Commands ##
ALL SQL REQUESTS HAVE STRICT SYNTAX!!!
That means all spacing and command names must be exactly as specified
ALL commands must be uppercase
Using command keywords inside requests IS NOT supported and will cause errors
ALL spaces inbetween values should be single
DO NOT try and order by a column name that does not exist in the table
### Sample Commands ###
SELECT name, year_start FROM nba_player_data.csv WHERE name=Darth Vader;
SELECT name, year_start FROM nba_player_data.csv ORDER BY name ASC;
SELECT name, year_start FROM nba_player_data JOIN nba_players ON nba_player_data.name = nba_players.Player;

DELETE FROM nba_player_data.csv;
DELETE FROM WHERE name = Shareef Abdur-Rahim;

UPDATE nba_player_data.csv SET name = Jake Warner, height = 68 WHERE name = Jim Beans;

INSERT INTO nba_player_data.csv (name,year_start,year_end,position,height,weight,birth_date,college) VALUES (Jim Beans,1995,2010,165,240, 'June 24, 1968', Duke University);
## Constraints ##
Only single JOINs and WHEREs are allowed
No functionality for column alias
Cannot insert multiple rows in one command