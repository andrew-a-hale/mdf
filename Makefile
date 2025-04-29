build: test kill
	go install ;

test: containers
	go test ./... ;

containers:
	podman run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 -d rabbitmq:4-management 
	sleep 10

kill:
	podman stop rabbitmq

