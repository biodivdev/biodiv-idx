# RRAPP-IDX

This application reads taxonomic and occurrence data from ElasticSearch as indexed by [dwc-bot-es](https://github.com/biodivdev/dwc-bot-es) and index analysis such as Extent of Occurrences, Area of Occupancy and others to provide a Extinction Risk Assessment as defined by IUCN methodology criterion B.

## Running

### Manual 

First, start your ElasticSearch 5.0. 

Then [run the dwc-bot-es](https://github.com/biodivdev/rrapp-idx/).

After dwc-bot-es has indexed the needed data, run this bot.

### Run with Docker

Run the docker container

    $ docker run -d -volume /etc/biodiv:/etc/biodiv:ro diogok/rrapp-idx

### Run the JAR

Download the latest jar from the [ realases page ](https://github.com/biodivdev/rrapp-idx/releases) and run it:

    $ java -server -jar rrapp-idx.jar

### Configuration

It will look for a configuration file on /etc/biodiv/config.ini or at the file defined by CONFIG environment variable.

The configuration file looks like the following:

    ELASTICSEARCH=http://localhost:9200
    INDEX=dwc
    SOURCE=lista_especies_flora_brasil
    LOOP=false

ElasticSearch tells to which elasticsearch server to connect. INDEX tells which ElasticSearch index to use. SOURCE define which checklist use from the Resources indexed. LOOP defines if the rrapp-idx should run in loop(true) or only once(false).

You can set the configuration override to use with environment variables, such as:

    $ CONFIG=/etc/biodiv/dwc.ini ELASTICSEARCH=http://localhost:9200 INDEX=dwc java -jar rrapp-idx.jar

Or all options combined:

    $ LOOP=true ELASTICSEARCH=http://localhost:9200 INDEX=dwc java -jar rrapp-idx.jar

If not running on a system with environment variables you can also set them using java properties, as such:

    $ java -jar -DLOOP=true -DCONFIG=/etc/biodiv/config.ini -DELASTICSEARCH=http://localhost:9200 -DINDEX=dwc rrapp-idx.jar

## License

MIT

