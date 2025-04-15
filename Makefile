# Build target
.PHONY: build
build:
	docker build -t monitor:latest .

# Clean up the Docker image
.PHONY: clean
clean:
	docker rmi monitor:latest
