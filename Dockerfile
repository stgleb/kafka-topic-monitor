# Use a more recent Go builder image
FROM golang:1.24 AS builder

# Set the current working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files if they exist
COPY go.mod go.sum ./

# Download the dependencies
RUN go mod download

# Copy the rest of your application source code
COPY . .

# Build the Go application
# Disable CGO to create a statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -o monitor ./cmd/monitor/main.go

# Start a new scratch container
FROM scratch

# Copy the binary into the scratch container
COPY --from=builder /app/monitor /monitor

# Set the command to run the application
ENTRYPOINT ["/monitor", "-bootstrap-servers", "kafka:9092", "-inactivity-days", "30", "-addr", ":8080", "-log-level", "info"]