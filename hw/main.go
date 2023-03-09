package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	//"reflect"

	"strconv"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "12345"
)

type Article struct { //какой джейсон планируем принимать
	Id        string `json:"Id"`
	Client    string `json:"Client"`
	Operation string `json:"Operation"`
	Amount    string `json:"Amount"`
}

// обработка запроса на создание
func createNewArticle(w http.ResponseWriter, r *http.Request) {
	reqBody, _ := ioutil.ReadAll(r.Body) //тело запроса
	var post Article

	var dbname = "postgres"
	var dbReplica = "replica"
	json.Unmarshal(reqBody, &post)

	json.NewEncoder(w).Encode(post)

	newData, err := json.Marshal(post)
	//print(post.Id)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(string(newData))
	}
	clientId, err := strconv.Atoi(post.Id)
	Amount, err := strconv.Atoi(post.Amount)
	Operation, err := strconv.ParseBool(post.Operation)
	//fmt.Println("var1 = ", reflect.TypeOf(clientId))
	//fmt.Println("var1 = ", reflect.TypeOf(Amount))
	go insertToDB(clientId, post.Client, Amount, 0, Operation, dbname)
	go insertToDB(clientId, post.Client, Amount, 0, Operation, dbReplica)

}

// обработка запроса на изменение
func changeArticle(w http.ResponseWriter, r *http.Request) {
	reqBody, _ := ioutil.ReadAll(r.Body)
	var dbname = "postgres"
	var dbReplica = "replica"
	var post Article
	json.Unmarshal(reqBody, &post)

	json.NewEncoder(w).Encode(post)

	newData, err := json.Marshal(post)
	//print(post.Id)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(string(newData))
	}
	clientId, err := strconv.Atoi(post.Id)
	Amount, err := strconv.Atoi(post.Amount)
	Operation, err := strconv.ParseBool(post.Operation)
	//fmt.Println("var1 = ", reflect.TypeOf(clientId))
	//fmt.Println("var1 = ", reflect.TypeOf(Amount))

	go insertToDB(clientId, post.Client, Amount, 1, Operation, dbname)
	go insertToDB(clientId, post.Client, Amount, 1, Operation, dbReplica)
	//fmt.Fprintf(w, resp)
}

// хендлер запросов, на post создание записи в бд на post1 изменение
func handleReqs() {
	r := mux.NewRouter().StrictSlash(true)
	r.HandleFunc("/post", createNewArticle).Methods("POST")  // здесь будет добавление записей в бд
	go r.HandleFunc("/post1", changeArticle).Methods("POST") // здесь их изменение
	log.Fatal(http.ListenAndServe(":8000", r))               //слушаем порт
}

// функция работы с бд, operationType передает тип операции 0 создать запист в БД, 1-изменить
// operation получаем из запроса, true пополнить false снять
func insertToDB(clientId int, clientName string, amount int, operationType int, operation bool, dbname string) string {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlconn)
	CheckError(err)

	defer db.Close()
	switch operationType {
	case 0: // кейс на добавление
		sql := `insert into test1 ("clientId", "clientName", "clientMoney") values (` + strconv.Itoa(clientId) + `,'` + clientName + `',` + strconv.Itoa(amount) + `)`
		fmt.Println(sql)
		insertStmt := sql
		_, e := db.Exec(insertStmt)
		CheckError(e)

		// check db
		err = db.Ping()
		CheckError(err)

		fmt.Println("inserted!")
		return "inserted"
	case 1: // кейс на изменение баланса
		var money int
		selectStmt := `select * from "test1" where "clientId"=$1 `
		Check, ee := db.Query(selectStmt, clientId)

		CheckError(ee)
		defer Check.Close()
		for Check.Next() {
			var name string
			var sqlClientId string
			err = Check.Scan(&sqlClientId, &name, &money)
			CheckError(err)
			if !operation {
				if money < 0 {
					fmt.Println("minus!")
					return "minus"
					break
				}
				if money < amount {
					fmt.Println("not enough money!")
					return "not money"
					break
				}
			}
			fmt.Println(sqlClientId, name, money)
		}

		CheckError(err)
		updateStmt := `update "test1" set "clientName"=$1, "clientMoney"=$2 where "clientId"=$3`
		fmt.Println(amount)
		fmt.Println(money)
		if operation { //зачисление денег
			_, e := db.Exec(updateStmt, clientName, amount+money, clientId)
			CheckError(e)
		} else { //снятие денег
			_, e := db.Exec(updateStmt, clientName, money-amount, clientId)
			CheckError(e)
		}

		// check db
		err = db.Ping()
		CheckError(err)

		fmt.Println("updated!")
		return "updated"
	case 2: //прост так
		fmt.Println("case 2")
	}
	return "0"
}

func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	handleReqs()
}
