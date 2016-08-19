package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/garyburd/redigo/redis"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/ini.v1"

	"time"

	yaml "gopkg.in/yaml.v2"
)

func usage() {
	usage := `
    help               --print this help
    list               --list all proxy groups
    list *             --list all proxy groups and its members
    list id            --list specified group id
    set * password     --set password for all redis instances with the same password
    set id password    --set password for specified redis instance
    exit               --exit

***** Any other commands not in lists will be Ignoooored! *****
`
	fmt.Println(usage)
}

type instance_info struct {
	id  string
	url string
}

type TwemproxyConfig struct {
	Listen         string   `yaml:"listen,omitempty"`
	Hash           string   `yaml:"hash,omitempty"`
	Distribution   string   `yaml:"distribution,omitempty"`
	AutoEjectHosts string   `yaml:"auto_eject_hosts,omitempty"`
	Redis          bool     `yaml:"redis,omitempty"`
	HashTag        string   `yaml:"hash_tag,omitempty"`
	RetryTimeout   int      `yaml:"server_retry_timeout,omitempty"`
	FailureLimit   int      `yaml:"server_failure_limit,omitempty"`
	Servers        []string `yaml:"servers,omitempty"`
}

var (
	cmd, para1, para2 string
	groups            []string
	instances         []string
	details           map[string][]instance_info
	passwdInfo        map[string]string
	twemproxyConfig   map[string]TwemproxyConfig
	timeout           int64
)

const f = "passwd.ini"

func main() {

	usage()

	cfg, err := ini.Load("app.conf")
	if err != nil {
		fmt.Println("load ini fail", err)
	}
	conf := cfg.Section("DEFAULT").Key("proxy").String()
	timeout = cfg.Section("DEFAULT").Key("timeout").MustInt64()

	groups, instances, details = loadTwemproxyConfig(conf)

	initPasswdIni()

	passwdInfo = getAuth()

	//for k, v := range passwdInfo {
	//	fmt.Println(k, v)
	//}

	for cmd != "exit" {

		fmt.Print(">>")
		fmt.Scanln(&cmd, &para1, &para2)

		switch strings.ToLower(cmd) {
		case "help":
			usage()
		case "exit":
			fmt.Println("Bye!")
			return
		case "list":
			// 一个参数
			list(para1, para2)
		case "set":
			setAuth(para1, para2)
		case "passwd":
			// 打印所有 redis 实例的密码1wa
			echoPwd()
		default:
			//do nothing
		}

		// 重置参数列表
		cmd, para1, para2 = "", "", ""

	}

}

func readYaml(path string, obj interface{}) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, obj)
}

func loadTwemproxyConfig(path string) (groups []string, instances []string, details map[string][]instance_info) {

	// details = make(map[string][]string)
	details = make(map[string][]instance_info)

	err := readYaml(path, &twemproxyConfig)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	for key := range twemproxyConfig {
		groups = append(groups, key)
	}
	sort.Strings(groups)

	id := 0
	for _, k := range groups {
		// ins := []string{}
		ins := []instance_info{}
		for _, v := range twemproxyConfig[k].Servers {
			x := v[:strings.LastIndex(v, ":")]
			// ins = append(ins, x)
			ins = append(ins, instance_info{fmt.Sprint(id), x})
			instances = append(instances, x)
			id++
		}
		// details[k] = ins
		details[k] = ins
	}

	return groups, instances, details

}

func list(para1, para2 string) {

	switch para1 {
	case "": //只列出代理组编号和组信息

		data := [][]string{}
		for i, groupName := range groups {
			data = append(data, []string{fmt.Sprint(i), groupName})
		}

		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"ID", "PROXY_GROUP_NAME"})

		for _, v := range data {
			table.Append(v)
		}
		table.Render() // Send output

	case "*": //列出所有组的detail

		for i, groupName := range groups {

			fmt.Printf("#%-4d%s\n", i, groupName)

			data := [][]string{}
			for _, v := range details[groupName] {
				data = append(data, []string{v.id, v.url})
			}

			table := tablewriter.NewWriter(os.Stdout)
			table.SetHeader([]string{"ID", "REDIS INSTANCE"})

			for _, v := range data {
				table.Append(v)
			}
			table.Render() // Send output

			fmt.Println()

		}

	default: //视参数为数值

		id, err := strconv.Atoi(para1)
		if err != nil {
			fmt.Println("not a valid id, you must be kidding me!")
			return
		}
		if id >= len(groups) {
			fmt.Println("id out of range, you must be kidding me!")
			return
		}
		fmt.Printf("#%-4d%s\n", id, groups[id])

		// groups[id] 得到组名
		data := [][]string{}
		for _, v := range details[groups[id]] {
			data = append(data, []string{v.id, v.url})
		}

		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"ID", "REDIS INSTANCE"})

		for _, v := range data {
			table.Append(v)
		}
		table.Render() // Send output

	}
}

func setAuth(para1, para2 string) {

	log.SetPrefix("...")

	if para2 == "" {
		log.Println("no passwd specified. you must be kidding me!")
		return
	}

	switch para1 {
	case "*":

		for _, url := range instances {
			authRedis(url, passwdInfo["DEFAULT:"+url], para2)
		}

	default:

		id, err := strconv.Atoi(para1)
		if err != nil {
			log.Println("not a valid id, you must be kidding me!")
			return
		}
		if id >= len(instances) {
			log.Println("id out of range, you must be kidding me!")
			return
		}
		url := instances[id]

		authRedis(url, passwdInfo["DEFAULT:"+url], para2)

	}
}

func saveAuth(url, passwd string) {

	log.SetPrefix("-- save auth --")
	cfg, err := ini.Load(f)
	if err != nil {
		fmt.Println("can not found file:", f)
	}

	cfg.Section("DEFAULT").Key(url).SetValue(passwd)

	err = cfg.SaveTo(f)

}

func getAuth() map[string]string {

	m := dumpAll(f)
	return m

}

func authRedis(url, currentPasswd, newPasswd string) {

	log.SetPrefix("...")
	log.Println("seting password for", url)

	r, err := redis.DialTimeout("tcp", url, time.Duration(timeout)*time.Second, time.Duration(timeout)*time.Second, time.Duration(timeout)*time.Second)
	if err != nil {
		log.Printf("%s (%d second). fail.", err, timeout)
		return
	}
	defer r.Close()

	if currentPasswd != "" {
		if _, err := r.Do("AUTH", currentPasswd); err != nil {
			log.Println("AUTH fail.")
			return
		}
	}

	if _, err := r.Do("CONFIG", "SET", "REQUIREPASS", newPasswd); err != nil {
		log.Println(err.Error())
		return
	}

	log.Println("seting password for", url, "success.")

	//save new password to passwd.ini
	saveAuth(strings.Replace(url, ":", "_", -1), newPasswd)
	//save new password to map in memory
	passwdInfo["DEFAULT:"+url] = newPasswd

}

func dumpAll(f string) map[string]string {

	log.SetPrefix("-- dumpall -- ")

	m := make(map[string]string)

	cfg, err := ini.Load(f)
	if err != nil {
		log.Println("load ini fail", err)
	}

	sections := cfg.SectionStrings()

	for _, section := range sections {
		keys := cfg.Section(section).KeyStrings()

		for _, key := range keys {
			// passwd.ini 用 _ , 代码中用 : 分隔 ip和 端口
			k := section + ":" + strings.Replace(key, "_", ":", -1)
			v := cfg.Section(section).Key(key).String()
			m[k] = v
		}

	}

	return m
}

func echoPwd() {

	data := [][]string{}
	for i, v := range instances {
		data = append(data, []string{fmt.Sprint(i), v, passwdInfo["DEFAULT:"+v]})
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"ID", "INSTANCE", "PASSWORD"})

	for _, v := range data {
		table.Append(v)
	}
	table.Render() // Send output

}

func initPasswdIni() {

	log.SetPrefix("-- init passwd.ini -- ")

	passwd := f

	//如果由 filename 指定的文件或目录存在则返回 true，否则返回 false
	_, err := os.Stat(passwd)
	if !(err == nil || os.IsExist(err)) {
		//文件不存在, 创建 passwd.ini
		os.Create(passwd)
		log.Println("file passwd.ini not exists, passwd.ini created.")
	} else {

		log.Println("file passwd.ini alreay exists.")
		log.Println("adjust entries according to proxy.yml.")
		//文件已存在, 读取 yaml, 获取 instance 实例 url
		//补全 passwd.ini 中的条目

		cfg, err := ini.Load(passwd)
		if err != nil {
			//this will not happen for passwd exists in this branch
			log.Println(err.Error())
		}

		for _, v := range instances {
			x := strings.Replace(v, ":", "_", -1)
			if !cfg.Section("DEFAULT").HasKey(x) {
				cfg.Section("DEFAULT").NewKey(x, "")
			}
		}

		cfg.SaveTo(passwd)

		log.Println("success.")

	}

}
