#Need to create persistant docker volumes eh. 

docker run -d --name=grafana -p 3000:3000 -v grafana-storage:/var/lib/grafana --restart unless-stopped --env GF_DASHBOARDS_MIN_REFRESH_INTERVAL=1  grafana/grafana 
docker run -d --name=influxdb -p 8086:8086 -v influx:qiu-storage:/var/lib/influxdb --restart unless-stopped influxdb/influxdb
