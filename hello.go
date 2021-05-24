package main

import (
	"fmt"
	"github.com/umeshdhaked/awesomeProject/packages/pubsub"
	"math/rand"
	"time"
)


var pubSubObj pubsub.IPubSub = pubsub.GetPubSub()

//func createTpc(id string) {
//	pubSubObj.CreateTopic(id)
//}
//
//func createSub(id string) {
//	pubSubObj.AddSubscription("1", id)
//}

func main() {

	fmt.Printf("This is the simulation of library with default hardcoded configurations :) \n\n\n\n ")

	//for i := 0 ; i<100 ; i++ {
	//	go createTpc(fmt.Sprintf("%v", i))
	//}
	//
	//for i := 0 ; i<100 ; i++ {
	//	go createSub(fmt.Sprintf("%v", i))
	//}
	//time.Sleep(2*time.Second)

	pubSubObj.CreateTopic("topic1")
	pubSubObj.AddSubscription("topic1", "sub1")
	pubSubObj.AddSubscription("topic1", "sub2")


	pubSubObj.Subscribe("sub1", SubscriberTypeA)
	pubSubObj.Subscribe("sub2", SubscriberTypeB)


	var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	for i:=0 ; i<100 ; i++{
		pubSubObj.Publish("topic1", fmt.Sprintf("Published randome Message: %v", seededRand.Int()))
	}

	time.Sleep(time.Minute*5)
}


func SubscriberTypeA(msg pubsub.Message) {
	defer pubSubObj.Ack(msg.MessageId(), "sub1")

	fmt.Println("SubscriberTypeA,  message : ", msg.Data() )

}

func SubscriberTypeB(msg pubsub.Message) {
	defer pubSubObj.Ack(msg.MessageId(), "sub2")

	fmt.Println("SubscriberTypeB,   message :", msg.Data())

	time.Sleep(time.Second*20)
}

