package main

import (
	"fmt"
	"github.com/colinmarc/hdfs"
	"github.com/colinmarc/hdfs/hadoopconf"
	krb "gopkg.in/jcmturner/gokrb5.v5/client"
	"gopkg.in/jcmturner/gokrb5.v5/config"
	"log"
	"strings"
)

func main() {
	//client, err := NewClient("slave5.cluster.local:8020")
	client, err := NewKerbosClient("slave5.cluster.local:8020")
	if err != nil {
		log.Fatal(" open connection error:", err)
	}
	defer client.Close()

	readFile(client, "/data/read.txt")
	//readFile(client, "/data/start.sh")
	//writeFile(client,"/data/1.txt","this is a test file.")
	//readFile(client, "/data/1.txt")
	//deleteFile(client, "/data/1.txt")
}

func writeFile(client *hdfs.Client, path string, content string) {
	writer, err := client.Create(path)
	if err != nil {
		log.Fatal(" create file error:", err)
	}
	defer writer.Close()

	n, err := writer.Write([]byte(content))
	if err != nil {
		log.Fatal(" write file error:", err)
	}
	log.Println(" write file len : ", n)

}
func readFile(client *hdfs.Client, path string) {
	log.Println(" read file : ", path)
	file, err := client.Open(path)
	if err != nil {
		log.Fatal(" open file error:", err)
	}
	defer file.Close()
	log.Println("File size = ", file.Stat().Size())

	buf := make([]byte, 128)
	var n int
	for n, err = file.Read(buf); n > 0; {
		fmt.Print(string(buf[0:n]))
		n, _ = file.Read(buf)
	}

	if err != nil {
		log.Fatal("read file error:", err)
	}
}
func deleteFile(client *hdfs.Client, path string) error {
	return client.Remove(path)
}

func NewClient(address string) (*hdfs.Client, error) {
	conf, err := hadoopconf.LoadFromEnvironment()
	if err != nil {
		return nil, err
	}

	options := hdfs.ClientOptionsFromConf(conf)
	if address != "" {
		options.Addresses = strings.Split(address, ",")
	}

	/*
		u, err := user.Current()
		if err != nil {
			return nil, err
		}
	*/

	options.User = "hdfs"
	return hdfs.NewClient(options)
}

func NewKerbosClient(address string) (*hdfs.Client, error) {
	//os.Setenv("HADOOP_HOME", "/usr/hdp/3.0.0.0-1634/hadoop") // This will resolve to testdata/conf.
	//os.Setenv("HADOOP_CONF_DIR", "/usr/hdp/3.0.0.0-1634/hadoop/etc/hadoop")
	conf, err := hadoopconf.LoadFromEnvironment()
	if err != nil {
		return nil, err
	}

	options := hdfs.ClientOptionsFromConf(conf)
	if address != "" {
		options.Addresses = strings.Split(address, ",")
	}
	username := "hdfs"
	options.KerberosClient = getKerberosClient(username)
	options.KerberosServicePrincipleName = "nn/_HOST" //"hdfs-c1"
	options.UseDatanodeHostname = true
	//options.User = username
	return hdfs.NewClient(options)
}

// getKerberosClient expects a ccache file for each user mentioned in the tests
// to live at /tmp/krb5cc_gohdfs_<username>, and krb5.conf to live at
// /etc/krb5.conf
func getKerberosClient(username string) *krb.Client {
	cfg, err := config.NewConfigFromString(`[libdefaults]
    renew_lifetime = 7d
    forwardable = true
	default_realm = BIGDATA
	ticket_lifetime = 2h
	dns_lookup_realm = false
    dns_lookup_kdc = false
	default_ccache_name = /tmp/krb5cc_%{uid}
	default_tgs_enctypes = aes128-cts-hmac-sha1-96
	default_tkt_enctypes = aes128-cts-hmac-sha1-96

	[domain_realm]
	.bigdata = BIGDATA

	[realms]
	BIGDATA = {
	admin_server = slave5.cluster.local
	kdc = slave5.cluster.local
	}`)
	//cfg,err = config.NewConfigFromString("[libdefaults]\nrenew_lifetime = 7d\nforwardable = true\ndefault_realm = BIGDATA\nticket_lifetime = 2h\ndns_lookup_realm = false\ndns_lookup_kdc = false\ndefault_ccache_name = /tmp/krb5cc_%{uid}\ndefault_tgs_enctypes = aes128-cts-hmac-sha1-96\ndefault_tkt_enctypes = aes128-cts-hmac-sha1-96\n[domain_realm]\n.bigdata = BIGDATA\n[realms]\nBIGDATA = {\nadmin_server = slave5.cluster.local\nkdc = slave5.cluster.local\n}")
	confs := "[libdefaults]\nrenew_lifetime = 7d\nforwardable = true\ndefault_realm = BIGDATA\nticket_lifetime = 2h\ndns_lookup_realm = false\ndns_lookup_kdc = false\ndefault_ccache_name = /tmp/krb5cc_%{uid}\ndefault_tgs_enctypes = aes128-cts-hmac-sha1-96\ndefault_tkt_enctypes = aes128-cts-hmac-sha1-96\n[domain_realm]\n.bigdata = BIGDATA\n[realms]\nBIGDATA = {\nadmin_server = slave5.cluster.local\nkdc = slave5.cluster.local\n}"
	cfg, err = config.NewConfigFromString(confs)
	strs := strings.Split(confs, "\n")
	fmt.Println(strs)
	//cfg, err := config.Load("/etc/krb5.conf")
	if err != nil {
		log.Fatal("Couldn't load krb config:", err)
	}
	//ktab, err := keytab.Load("/etc/security/keytabs/hdfs.headless.keytab")
	//if err != nil {
	//	log.Fatal("Couldn't load keytab :", err)
	//}
	//client := krb.NewClientWithKeytab("hdfs-c1","BIGDATA",ktab)

	client := krb.NewClientWithPassword("jiadx/jiadx-M4800", "BIGDATA", "123456a?")
	err = client.WithConfig(cfg).Login()
	if err != nil {
		log.Fatal("Couldn't initialize krb client:", err)
	}

	return &client
}
