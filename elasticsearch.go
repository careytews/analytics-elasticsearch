//
// ElasticSearch loader for the analytics cluster.  Takes events on input queue
// and restructures for loading into an ElasticSearch index.
// One object per event.
//
// No output queues are used.
//

package main

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"context"

	"github.com/olivere/elastic"
	"github.com/prometheus/client_golang/prometheus"
	dt "github.com/trustnetworks/analytics-common/datatypes"
	"github.com/trustnetworks/analytics-common/utils"
	"github.com/trustnetworks/analytics-common/worker"
)

const pgm = "elasticsearch"

type work struct {
	conn           *elastic.Client
	bps            *elastic.BulkProcessor
	url            string
	es_template    string
	es_write_alias string
	es_read_alias  string
	es_shards      int
	es_object      string
	ttl            int
	eventLatency   *prometheus.SummaryVec
	recvLabels     prometheus.Labels
}

// Can use this to keep track of ES failures.
// FIXME: Should integrate stats.
var afters, failures int64
var afterRequests int64

func afterFn(executionId int64, requests []elastic.BulkableRequest,
	response *elastic.BulkResponse, err error) {

	curAfters := atomic.AddInt64(&afters, 1)

	if err != nil {
		atomic.AddInt64(&failures, 1)
	}
	curFailures := atomic.LoadInt64(&failures)

	curReqs := atomic.AddInt64(&afterRequests, int64(len(requests)))

	if curReqs%100000 == 0 {
		utils.Log("Stats: batches=%d failed=%d objects=%d", curAfters,
			curFailures, curReqs)
	}

}

func (s *work) elasticInit() error {

	// Open ElasticSearch connection.

	for {

		var err error
		s.conn, err = elastic.NewClient(elastic.SetURL(s.url))
		if err != nil {
			utils.Log("Elasticsearch connection: %s", err.Error())
			continue
		}

		break

		time.Sleep(1 * time.Second * 5)

	}

	for {

		tmplExists, err := s.conn.IndexTemplateExists(s.es_template).
			Do(context.Background())
		if err != nil {
			utils.Log("Elasticsearch: %s", err.Error())
			continue
		}

		number_of_shards := strconv.Itoa(s.es_shards)
		template := `{
		  "template": "` + s.es_template + `*",
		  "aliases": {
		    "` + s.es_read_alias + `": {}
		  },
		  "settings": {
		    "index.mapping.total_fields.limit": 5000,
		    "number_of_shards": ` + number_of_shards + `,
		    "number_of_replicas": 1,
		    "routing.allocation.total_shards_per_node": ` + number_of_shards + `,
		    "routing.allocation.include.box_type": "hot"
		  },
		  "mappings": {
		    "` + s.es_object + `": {
		      "properties": {
		        "id": {
		          "type": "keyword"
		        },
		        "time": {
		          "type": "date"
		        },
		        "url": {
		          "type": "keyword"
		        },
		        "action": {
		          "type": "keyword"
		        },
		        "device": {
		          "type": "keyword"
		        },
		        "network": {
		          "type": "keyword"
		        },
		        "origin": {
		          "type": "keyword"
		        },
		        "risk": {
		          "type": "float"
		        },
				"operations" : {
					"properties": {
						"unknown": {
							"type": "keyword"
						}
					}
				},
		        "dns": {
		          "properties": {
		            "type": {
		              "type": "keyword"
		            },
		            "query": {
		              "properties": {
		                "name": {
		                  "type": "keyword"
		                },
		                "type": {
		                  "type": "keyword"
		                },
		                "class": {
		                  "type": "keyword"
		                }
		              }
		            },
		            "answer": {
		              "properties": {
		                "name": {
		                  "type": "keyword"
		                },
		                "type": {
		                  "type": "keyword"
		                },
		                "class": {
		                  "type": "keyword"
		                },
		                "address": {
		                  "type": "keyword"
		                }
		              }
		            }
		          }
		        },
		        "http": {
		          "properties": {
		            "method": {
		              "type": "keyword"
		            },
		            "status": {
		              "type": "keyword"
		            },
		            "code": {
		              "type": "integer"
		            },
		            "header": {
		              "properties": {
		                "User-Agent": {
		                  "type": "keyword"
		                },
		                "Host": {
		                  "type": "keyword"
		                },
		                "Content-Type": {
		                  "type": "keyword"
		                },
		                "Server": {
		                  "type": "keyword"
		                },
		                "Connection": {
		                  "type": "keyword"
		                }
		              }
		            }
		          }
		        },
		        "ftp": {
		          "properties": {
		            "command": {
		              "type": "keyword"
		            },
		            "status": {
		              "type": "integer"
		            },
		            "text": {
		              "type": "text"
		            }
		          }
		        },
		        "icmp": {
		          "properties": {
		            "type": {
		              "type": "integer"
		            },
		            "code": {
		              "type": "integer"
		            }
		          }
		        },
		        "sip": {
		          "properties": {
		            "method": {
		              "type": "keyword"
		            },
		            "from": {
		              "type": "keyword"
		            },
		            "to": {
		              "type": "keyword"
		            },
		            "status": {
		              "type": "keyword"
		            },
		            "code": {
		              "type": "integer"
		            }
		          }
		        },
		        "smtp": {
		          "properties": {
		            "command": {
		              "type": "keyword"
		            },
		            "from": {
		              "type": "keyword"
		            },
		            "to": {
		              "type": "keyword"
		            },
		            "status": {
		              "type": "keyword"
		            },
		            "text": {
		              "type": "text"
		            },
		            "code": {
		              "type": "integer"
		            }
		          }
		        },
		        "ntp": {
		          "properties": {
		            "version": {
		              "type": "integer"
		            },
		            "mode": {
		              "type": "integer"
		            }
		          }
		        },
		        "unrecognised_payload": {
		          "properties": {
		            "sha1": {
		              "type": "keyword"
		            },
		            "length": {
		              "type": "integer"
		            }
		          }
		        },
		        "src": {
		          "properties": {
		            "ipv4": {
		              "type": "ip"
		            },
		            "ipv6": {
		              "type": "ip"
		            },
		            "tcp": {
		              "type": "integer"
		            },
		            "udp": {
		              "type": "integer"
		            }
		          }
		        },
		        "dest": {
		          "properties": {
		            "ipv4": {
		              "type": "ip"
		            },
		            "ipv6": {
		              "type": "ip"
		            },
		            "tcp": {
		              "type": "integer"
		            },
		            "udp": {
		              "type": "integer"
		            }
		          }
		        },
		        "location": {
		          "properties": {
		            "src": {
		              "properties": {
		                "city": {
		                  "type": "keyword"
		                },
		                "iso": {
		                  "type": "keyword"
		                },
		                "country": {
		                  "type": "keyword"
		                },
		                "asnum": {
		                  "type": "integer"
		                },
		                "asorg": {
		                  "type": "keyword"
		                },
		                "position": {
		                  "type": "geo_point"
		                },
		                "accuracy": {
		                  "type": "integer"
		                },
		                "postcode": {
		                  "type": "keyword"
		                }
		              }
		            },
		            "dest": {
		              "properties": {
		                "city": {
		                  "type": "keyword"
		                },
		                "iso": {
		                  "type": "keyword"
		                },
		                "country": {
		                  "type": "keyword"
		                },
		                "asnum": {
		                  "type": "integer"
		                },
		                "asorg": {
		                  "type": "keyword"
		                },
		                "position": {
		                  "type": "geo_point"
		                },
		                "accuracy": {
		                  "type": "integer"
		                },
		                "postcode": {
		                  "type": "keyword"
		                }
		              }
		            }
		          }
		        },
		        "indicators": {
		          "properties": {
		            "id": {
		              "type": "keyword"
		            },
		            "type": {
		              "type": "keyword"
		            },
		            "value": {
		              "type": "keyword"
		            },
		            "description": {
		              "type": "keyword"
		            },
		            "category": {
		              "type": "keyword"
		            },
		            "author": {
		              "type": "keyword"
		            },
		            "source": {
		              "type": "keyword"
		            },
		            "probability": {
		              "type": "float"
		            }
		          }
		        }
		      }
		    }
		  }
		}`

		ipt, err := s.conn.IndexPutTemplate(s.es_template).
			BodyString(template).
			Do(context.Background())

		if err != nil {
			utils.Log("(PutTemplateFromJson) (ignored): %s", err.Error())
		} else {
			if !ipt.Acknowledged {
				utils.Log("Create template not acknowledged.")
			} else {
				utils.Log("Template created.")
			}
		}

		if tmplExists {
			utils.Log("Index Template Update Complete")
			time.Sleep(time.Second * 5)
			break
		}

		time.Sleep(time.Second * 1)

		ci, err := s.conn.CreateIndex(s.es_write_alias + "-000001").
			Do(context.Background())

		if err != nil {
			utils.Log("(CreateEmptyIndex) (ignored): %s", err.Error())
		} else {
			if !ci.Acknowledged {
				utils.Log("Create index not acknowledged.")
			} else {
				utils.Log("Index created.")
			}
		}

		time.Sleep(time.Second * 1)

		ara, err := s.conn.Alias().Add(s.es_write_alias+"-000001", s.es_write_alias).
			Do(context.Background())

		if err != nil {
			utils.Log("(AddWriteAlias) (ignored): %s", err.Error())
		} else {
			if !ara.Acknowledged {
				utils.Log("Add write alias not acknowledged.")
			} else {
				utils.Log("Write Alias Added")
			}
		}

	}

	time.Sleep(time.Second * 1)

	var err error
	s.bps, err = s.conn.BulkProcessor().
		Name("Worker-1").
		Workers(5).
		BulkActions(25).
		BulkSize(5 * 1024 * 1024).
		FlushInterval(1 * time.Second).
		After(afterFn).
		Do(context.Background())
	if err != nil {
		utils.Log("BulkProcess: %s\n")

		// FIXME: Need to retry that one.
		os.Exit(1)
	}

	return nil

}

func (s *work) init() error {

	// Get configuration values from environment variables.
	s.url = utils.Getenv("ELASTICSEARCH_URL", "http://localhost:9200")
	s.es_read_alias = utils.Getenv("ELASTICSEARCH_READ_ALIAS", "cyberprobe")
	s.es_write_alias = utils.Getenv("ELASTICSEARCH_WRITE_ALIAS", "active-cyberprobe")
	s.es_template = utils.Getenv("ELASTICSEARCH_TEMPLATE", "active-cyberprobe")
	s.es_shards, _ = strconv.Atoi(utils.Getenv("ELASTICSEARCH_SHARDS", "3"))
	s.es_object = utils.Getenv("ELASTICSEARCH_OBJECT", "observation")
	s.ttl, _ = strconv.Atoi(utils.Getenv("ELASTICSEARCH_TTL", "2419200"))
	//confiuration specific to prometheus stats
	s.recvLabels = prometheus.Labels{"store": pgm}
	s.eventLatency = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "event_latency",
			Help: "Latency from cyberprobe to store",
		},
		[]string{"store"},
	)

	prometheus.MustRegister(s.eventLatency)

	return s.elasticInit()

}

type ObQuery struct {
	Name  []string `json:"name,omitempty"`
	Class []string `json:"class,omitempty"`
	Type  []string `json:"type,omitempty"`
}

type ObAnswer struct {
	Name    []string `json:"name,omitempty"`
	Class   []string `json:"class,omitempty"`
	Type    []string `json:"type,omitempty"`
	Address []string `json:"address,omitempty"`
}

type ObIndicators struct {
	Id          []string `json:"id,omitempty"`
	Type        []string `json:"type,omitempty"`
	Value       []string `json:"value,omitempty"`
	Description []string `json:"description,omitempty"`
	Category    []string `json:"category,omitempty"`
	Author      []string `json:"author,omitempty"`
	Source      []string `json:"source,omitempty"`
	Probability []float32 `json:"probability,omitempty"`
}

type ObHttp struct {
	Method string            `json:"method,omitempty"`
	Header map[string]string `json:"header,omitempty"`
	Status string            `json:"status,omitempty"`
	Code   int               `json:"code,omitempty"`
}

type ObFtp struct {
	Command string   `json:"command,omitempty"`
	Status  int      `json:"status,omitempty"`
	Text    []string `json:"text,omitempty"`
}

type ObIcmp struct {
	Type int `json:"type"`
	Code int `json:"code"`
}

type ObDns struct {
	Query  *ObQuery  `json:"query,omitempty"`
	Answer *ObAnswer `json:"answer,omitempty"`
	Type   string    `json:"type,omitempty"`
}

type ObSip struct {
	Method string `json:"method,omitempty"`
	From   string `json:"from,omitempty"`
	To     string `json:"to,omitempty"`
	Status string `json:"status,omitempty"`
	Code   int    `json:"code,omitempty"`
}

type ObSmtp struct {
	Command string   `json:"command,omitempty"`
	From    string   `json:"from,omitempty"`
	To      []string `json:"to,omitempty"`
	Status  int      `json:"status,omitempty"`
	Text    []string `json:"text,omitempty"`
}

type ObNtp struct {
	Version int `json:"version"`
	Mode    int `json:"mode"`
}

type ObUnrecog struct {
	Sha1   string `json:"sha1,omitempty"`
	Length int    `json:"length,omitempty"`
}

type Observation struct {
	Id      string `json:"id,omitempty"`
	Action  string `json:"action,omitempty"`
	Device  string `json:"device,omitempty"`
	Network string `json:"network,omitempty"`
	Origin  string `json:"origin,omitempty"`
	Time    string `json:"time,omitempty"`

	Src  map[string][]string `json:"src,omitempty"`
	Dest map[string][]string `json:"dest,omitempty"`

	Url string `json:"url,omitempty"`

	Http *ObHttp `json:"http,omitempty"`

	Icmp *ObIcmp `json:"icmp,omitempty"`

	Dns *ObDns `json:"dns,omitempty"`

	Ftp *ObFtp `json:"ftp,omitempty"`

	Sip *ObSip `json:"sip,omitempty"`

	Smtp *ObSmtp `json:"smtp,omitempty"`

	Ntp *ObNtp `json:"ntp,omitempty"`

	Unrecognised *ObUnrecog `json:"unrecognised_payload,omitempty"`

	Location *dt.LocationInfo `json:"location,omitempty"`

	Indicators *ObIndicators `json:"indicators,omitempty"`

	Risk float64 `json:"risk"`

	Operations map[string]string `json:"operations,omitempty"`
}

func Convert(ev *dt.Event) *Observation {

	ob := &Observation{}
	
	ob.Id = ev.Id
	ob.Action = ev.Action
	ob.Device = ev.Device
	ob.Network = ev.Network
	ob.Origin = ev.Origin
	ob.Time = ev.Time
	ob.Url = ev.Url
	ob.Location = ev.Location
	ob.Risk = ev.Risk

	var query *ObQuery
	var answer *ObAnswer

	if ev.DnsMessage != nil {

		msg := ev.DnsMessage

		if msg.Query != nil {
			query = &ObQuery{}
			for _, val := range msg.Query {
				query.Name = append(query.Name, val.Name)
				query.Type = append(query.Type, val.Type)
				query.Class = append(query.Class, val.Class)
			}
		}

		if msg.Answer != nil {
			answer = &ObAnswer{}
			for _, val := range msg.Answer {
				answer.Name = append(answer.Name, val.Name)
				answer.Type = append(answer.Type, val.Type)
				answer.Class = append(answer.Class, val.Class)
				answer.Address = append(answer.Address, val.Address)
			}

		}
	}

	switch ev.Action {
	case "http_request":
		ob.Http = &ObHttp{
			Method: ev.HttpRequest.Method,
			Header: ev.HttpRequest.Header,
		}
	case "http_response":
		ob.Http = &ObHttp{
			Status: ev.HttpResponse.Status,
			Code:   ev.HttpResponse.Code,
			Header: ev.HttpResponse.Header,
		}
	case "ftp_command":
		ob.Ftp = &ObFtp{
			Command: ev.FtpCommand.Command,
		}
	case "ftp_response":
		ob.Ftp = &ObFtp{
			Status: ev.FtpResponse.Status,
			Text:   ev.FtpResponse.Text,
		}
	case "icmp":
		ob.Icmp = &ObIcmp{
			Type: ev.Icmp.Type,
			Code: ev.Icmp.Code,
		}
	case "dns_message":
		ob.Dns = &ObDns{
			Query:  query,
			Answer: answer,
			Type:   ev.DnsMessage.Type,
		}
	case "sip_request":
		ob.Sip = &ObSip{
			Method: ev.SipRequest.Method,
			From:   ev.SipRequest.From,
			To:     ev.SipRequest.To,
		}
	case "sip_response":
		ob.Sip = &ObSip{
			Code:   ev.SipResponse.Code,
			Status: ev.SipResponse.Status,
			From:   ev.SipResponse.From,
			To:     ev.SipResponse.To,
		}
	case "smtp_command":
		ob.Smtp = &ObSmtp{
			Command: ev.SmtpCommand.Command,
		}
	case "smtp_response":
		ob.Smtp = &ObSmtp{
			Status: ev.SmtpResponse.Status,
			Text:   ev.SmtpResponse.Text,
		}
	case "smtp_data":
		ob.Smtp = &ObSmtp{
			From: ev.SmtpData.From,
			To:   ev.SmtpData.To,
		}
	case "ntp_timestamp":
		ob.Ntp = &ObNtp{
			Version: ev.NtpTimestamp.Version,
			Mode:    ev.NtpTimestamp.Mode,
		}
	case "ntp_control":
		ob.Ntp = &ObNtp{
			Version: ev.NtpControl.Version,
			Mode:    ev.NtpControl.Mode,
		}
	case "ntp_private":
		ob.Ntp = &ObNtp{
			Version: ev.NtpPrivate.Version,
			Mode:    ev.NtpPrivate.Mode,
		}
	case "unrecognised_stream":
		ob.Unrecognised = &ObUnrecog{
			Sha1:   ev.UnrecognisedStream.PayloadHash,
			Length: ev.UnrecognisedStream.PayloadLength,
		}
	case "unrecognised_datagram":
		ob.Unrecognised = &ObUnrecog{
			Sha1:   ev.UnrecognisedDatagram.PayloadHash,
			Length: ev.UnrecognisedDatagram.PayloadLength,
		}
		// All other types are dealt with by the common cases above.
	}

	// Indicators.
	if ev.Indicators != nil {
		ob.Indicators = &ObIndicators{}
		for _, val := range *ev.Indicators {
			ob.Indicators.Id = append(ob.Indicators.Id, val.Id)
			ob.Indicators.Type =
				append(ob.Indicators.Type, val.Type)
			ob.Indicators.Value =
				append(ob.Indicators.Value, val.Value)
			ob.Indicators.Category =
				append(ob.Indicators.Category, val.Category)
			ob.Indicators.Description =
				append(ob.Indicators.Description,
					val.Description)
			ob.Indicators.Author =
				append(ob.Indicators.Author, val.Author)
			ob.Indicators.Source =
				append(ob.Indicators.Source, val.Source)
			ob.Indicators.Probability =
				append(ob.Indicators.Probability,
				val.Probability)
		}
	}

	ob.Src = make(map[string][]string, 0)
	ob.Dest = make(map[string][]string, 0)

	if ev.Src != nil {

		for _, val := range ev.Src {

			var cls, addr string

			val_parts := strings.SplitN(val, ":", 2)
			cls = val_parts[0]
			if len(val_parts) > 1 {
				addr = val_parts[1]
			} else {
				addr = ""
			}

			if _, ok := ob.Src[cls]; !ok {
				ob.Src[cls] = make([]string, 0)
			}

			ob.Src[cls] = append(ob.Src[cls], addr)

		}

	}

	if ev.Dest != nil {

		for _, val := range ev.Dest {

			var cls, addr string

			val_parts := strings.SplitN(val, ":", 2)
			cls = val_parts[0]
			if len(val_parts) > 1 {
				addr = val_parts[1]
			} else {
				addr = ""
			}

			if _, ok := ob.Dest[cls]; !ok {
				ob.Dest[cls] = make([]string, 0)
			}

			ob.Dest[cls] = append(ob.Dest[cls], addr)

		}

	}

	if ev.Operations != nil {
		ob.Operations = make(map[string]string)
		for _, op := range *ev.Operations {
			for k, v := range op.Values {
				ob.Operations[op.Name+"."+k] = v
			}
		}
	}

	return ob

}

func (h *work) Handle(msg []uint8, w *worker.Worker) error {
	
	var ev dt.Event

	err := json.Unmarshal(msg, &ev)
	if err != nil {
		utils.Log("Couldn't unmarshal json: %s", err.Error())
		return nil
	}

	// Debug: Dump message on output if device is 'debug'.
	if ev.Device == "debug" {
		utils.Log("before: %s", string(msg))
	}

	ob := Convert(&ev)

	err = h.Output(*ob, ev.Id)
	if err != nil {
		utils.Log("index failed: %s", err.Error())
		return nil
	}

	// Debug: Dump message on output if device is 'debug'.
	if ob.Device == "debug" {
		b, _ := json.Marshal(&ob)
		utils.Log("output: %s", string(b))
	}

	return nil

}

func (h *work) Output(ob Observation, id string) error {

	/*
		stf, err := json.Marshal(ob)
		if err != nil {
			utils.Log("Encoding error: %s", err.Error())
		}

		utils.Log("%s", stf)

		return nil
	*/

	bir := elastic.NewBulkIndexRequest().
		Doc(ob).
		Id(id).
		Index(h.es_write_alias).
		Type(h.es_object)

	ts := time.Now().UnixNano()
	go h.recordLatency(ts, ob)

	h.bps.Add(bir)

	return nil

}

func (h *work) recordLatency(ts int64, ob Observation) {
	obsTime, err := time.Parse(time.RFC3339, ob.Time)
	if err != nil {
		utils.Log("Date Parse Error: %s", err.Error())
	}
	latency := ts - obsTime.UnixNano()
	h.eventLatency.With(h.recvLabels).Observe(float64(latency))
}

func main() {

	utils.LogPgm = pgm
	var w worker.QueueWorker
	var s work

	var input string
	var output []string

	if len(os.Args) > 0 {
		input = os.Args[1]
	}
	if len(os.Args) > 2 {
		output = os.Args[2:]
	}

	// context to handle control of subroutines
	ctx := context.Background()
	ctx, cancel := utils.ContextWithSigterm(ctx)
	defer cancel()
	
	err := w.Initialise(ctx, input, output, pgm)
	if err != nil {
		utils.Log("init: %s", err.Error())
		return
	}
	
	// Initialise.
	err = s.init()
	if err != nil {
		utils.Log("init: %s", err.Error())
		return
	}

	utils.Log("Initialisation complete.")

	// Invoke Wye event handling.
	err = w.Run(ctx, &s)
	if err != nil {
		utils.Log("error: Event handling failed with err: %s", err.Error())
	}

}
