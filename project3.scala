import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import scala.util.Random
import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer

object project3 {
	def main(args: Array[String]){

		case class CreateTopology() 

		println("project3 - Chord DHT")
		class Master(numOfNodes: Int, numOfRequests: Int) extends Actor{

			//var gossipList: ArrayBuffer[String] = new ArrayBuffer[String]

			def receive = {
				case CreateTopology() =>
					for(i <- 0 until numOfNodes){
							var myID: String = "ipaddress".concat(i.toString)
							val act = context.actorOf(Props(new Node(myID, numOfRequests),name=myID)
							act ! Join()
						}

			}
		}

		class Node(myID: String, numOfRequests: Int) 
			extends Actor{

			var 
			// var neighborList: ArrayBuffer[String] = new ArrayBuffer[String]
			

			def receive = {
				case Gossip(msg: String) =>
					
			}
		}

		def sha1(s: String): String = {
			val md = MessageDigest.getInstance("SHA-1")
			val digest: Array[Byte] = md.digest(s.getBytes)
			var sb: StringBuffer = new StringBuffer
			digest.foreach { digest =>
				var hex = Integer.toHexString(digest & 0xff)
				if (hex.length == 1) sb.append('0')
				sb.append(hex)
			}
			sb.toString()
		}

		if(args.size<2){
			println("Enter valid number of inputs!!")
		} else {
			val system = ActorSystem("MasterSystem")
			val master = system.actorOf(Props(new Master(args(0).toInt, args(1).toInt), name = "master")
			master ! CreateTopology()
		}

	}	
}