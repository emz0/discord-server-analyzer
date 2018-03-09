# Discord server analyzer
A tool for downloading and analyzing [Discord](https://discordapp.com/) messages. 

### Real example
Real example that includes visualization of the results + more details to the whole process:
[JH Discord server analysis](https://plot.ly/~emzo/29/jumpers-heaven-discord-server-analysis/)


### Downloader
Connects to a Discord server specified in `settings.py`, downloads messages and stores them in a PostgreSQL database.

### Analyzer
Reads data from the database which, analyzes them and exports results to an output folder in csv format.


### Install
TBA

### Run
`python run.py downloader`  
`python run.py analyzer`
