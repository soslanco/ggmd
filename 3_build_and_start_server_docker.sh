#!/bin/sh

cd server

BIN=server
SERVICE=atlant

if [ -f ./.mode-PRODUCTION ]; then
	MODE='production'
	MONGO_URI="mongodb://secret:secret@secret.com"
	MONGO_DB=crypto
	RPC_ADDR=9090
else
	MODE='development'
	MONGO_URI="mongodb://root:example@mongo"
	MONGO_DB=crypto
	RPC_ADDR=9090
fi

echo "Mode: $MODE"


rm -f $BIN

CGO_ENABLED=0 go build

if [ -f "./$BIN" ]; then
	# Stop service
	docker ps -qf "name=$SERVICE" | xargs -I {} docker kill --signal=SIGTERM {} >/dev/null 2>&1
	echo -n "Wait until service stoped"
	while docker ps -f "name=$SERVICE" | grep "$SERVICE" >/dev/null; do
		sleep 1
		echo -n "."
	done
	echo

	# Delete container
	docker service rm $SERVICE >/dev/null 2>&1
	docker ps -aqf "name=$SERVICE" | xargs -I {} docker rm {} >/dev/null 2>&1
	echo -n "Wait until container deleted"
	while docker ps -af "name=$SERVICE" | grep "$SERVICE" >/dev/null; do
		sleep 1
		echo -n "."
	done
	echo

	# Delete image
	docker rmi soslanco/$SERVICE

	# Build image
	docker build -t soslanco/$SERVICE -f Dockerfile  .

	# Start service
	docker service create --name $SERVICE --network mongo_default --replicas 2 --restart-max-attempts 2 --publish $RPC_ADDR:$RPC_ADDR \
		-e "GGMD_MONGO_URI=$MONGO_URI" \
		-e "GGMD_MONGO_DB=$MONGO_DB" \
		-e "RPC_ADDR=:$RPC_ADDR" \
		--restart-condition "on-failure" soslanco/$SERVICE
fi
