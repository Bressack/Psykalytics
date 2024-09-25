package main

import (
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type Event struct {
	Id         string    `json:"id"`
	Type       string    `json:"type"`
	Session_id string    `json:"session_id"`
	Timestamp  time.Time `json:"timestamp"`
	Sint       int32     `json:"sint"`
	Lint       int64     `json:"lint"`
	Sstr       string    `json:"sstr"`
	Lstr       string    `json:"lstr"`
}

type DTOSession struct {
	Id     string  `json:"id"`
	Events []Event `json:"events"`
}

type DTOSessionId struct {
	Session_id string `json:"session_id"`
}

type DTOEvent struct {
	Type string `json:"type"`
	Sint int32  `json:"sint"`
	Lint int64  `json:"lint"`
	Sstr string `json:"sstr"`
	Lstr string `json:"lstr"`
}

type DTOCheckEvent struct {
	Timestamp time.Time `json:"timestamp"`
	Sstr      string    `json:"sstr"`
}

/*
	routes:
		POST /send recieve event from client
		GET /session send full session datas
*/

func InsertEvent(conn driver.Conn) gin.HandlerFunc {
	return func(c *gin.Context) {
		var err error
		var session_id string = c.Request.Header.Get("session_id")
		var datas DTOEvent
		err = c.BindJSON(&datas)
		if err != nil {
			log.Println(err)
			c.Status(400)
			return
		}
		var isinit bool = false
		if len(session_id) == 0 {
			session_id = uuid.New().String()
			isinit = true
			datas.Type = "start"
		}
		querystr := fmt.Sprintf("INSERT INTO events(id,type,session_id,timestamp,sint,lint,sstr,lstr) VALUES (generateUUIDv4(), '%s', '%s', now(), %d, %d, '%s', '%s');",
			datas.Type, session_id, datas.Sint, datas.Lint, datas.Sstr, datas.Lstr)
		err = conn.AsyncInsert(c, querystr, false)
		if err != nil {
			log.Println(err)
			c.Status(500)
			return
		}
		if isinit {
			c.JSON(200, DTOSessionId{session_id})
		} else {
			c.Status(200)
		}
	}
}

func GetSession(conn driver.Conn) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")
		// SQL injections bonjouuuuur
		querystr := fmt.Sprintf("SELECT * FROM events WHERE session_id='%s'", id)
		rows, err := conn.Query(c, querystr)
		if err != nil {
			log.Println(err)
			c.JSON(400, "Bad Request")
			return
		}
		var session DTOSession
		session.Id = id
		for rows.Next() {
			var e Event
			if err := rows.Scan(
				&e.Id,
				&e.Type,
				&e.Session_id,
				&e.Timestamp,
				&e.Sint,
				&e.Lint,
				&e.Sstr,
				&e.Lstr,
			); err != nil {
				log.Println(err)
				c.JSON(400, "Bad Request")
				return
			}
			session.Events = append(session.Events, e)
		}
		c.JSON(200, session)
	}
}

func GetEventFromSession(conn driver.Conn) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")
		sint := c.Param("sint")
		// SQL injections bonjouuuuur
		querystr := fmt.Sprintf("SELECT timestamp,sstr FROM events WHERE session_id='%s' AND sint=%s", id, sint)
		row := conn.QueryRow(c, querystr)
		var e DTOCheckEvent
		if err := row.Scan(&e.Timestamp, &e.Sstr); err != nil {
			c.JSON(404, "Event Not Found")
			return
		}
		c.JSON(200, e)
	}
}

func main() {
	conn, err := Connect()
	if err != nil {
		panic((err))
	}
	router := gin.New()
	// router := gin.Default()
	route_send := router.Group("/send")
	{
		route_send.POST("", InsertEvent(conn))
	}
	route_session := router.Group("/session")
	{
		route_session.GET(":id", GetSession(conn))
		route_session.GET(":id/event/:sint", GetEventFromSession(conn))
	}
	if router.Run("0.0.0.0:8080") != nil {
		log.Println(err)
	}
}
