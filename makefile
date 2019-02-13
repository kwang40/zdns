all: extract-fqdn lookup-ip zdns/zdns

zdns/zdns:
	cd zdns && go build

clean:
	rm -f zdns/zdns extract-fqdn lookup-ip

install: zdns/zdns
	cd zdns && go install

.PHONY: extract-fqdn lookup-ip zdns/zdns clean

extract-fqdn:
	go build redis-store-url/extract-fqdn.go

lookup-ip:
	go build redis-lookup-ip/lookup-ip.go
