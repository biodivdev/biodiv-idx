#!/bin/bash

SERVER="163.172.130.4"

TAXADATA_URL="http://$SERVER/taxadata/api/v2" ELASTICSEARCH_URL="http://$SERVER/elasticsearch/biodiv" DWC_SERVICES_URL="http://$SERVER/dwcservices/api/v1" DWC_BOT_URL="http://$SERVER/dwcbot" lein run "$1"
