import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging

import scala.collection.mutable.ListBuffer

//class GenericCell(var prev: BigInt, var next: BigInt)
//class GenericMap extends scala.collection.mutable.HashMap[BigInt, GenericCell]

class GenericCell(var prev: BigInt, var next: BigInt)
class GenericMap extends scala.collection.mutable.HashMap[BigInt, GenericCell]
case class Message(groupId: BigInt, msg: String)


/**
  * GenericService is an example app service for the actor-based KVStore/KVClient.
  * This one stores Generic Cell objects in the KVStore.  Each app server allocates new
  * GenericCells (allocCell), writes them, and reads them randomly with consistency
  * checking (touchCell).  The allocCell and touchCell commands use direct reads
  * and writes to bypass the client cache.  Keeps a running set of Stats for each burst.
  *
  * @param myNodeID sequence number of this actor/server in the app tier
  * @param numNodes total number of servers in the app tier
  * @param storeServers the ActorRefs of the KVStore servers
  * @param burstSize number of commands per burst
  */


class GroupServer (val myNodeID: Int, val numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int) extends Actor {
  val generator = new scala.util.Random
  val cellstore = new KVClient(storeServers)
  val dirtycells = new AnyMap
  val localWeight: Int = 70
  val log = Logging(context.system, this)

  var stats = new Stats
  var allocated: Int = 0
  var endpoints: Option[Seq[ActorRef]] = None


  //val myGroupID:BigInt = (Math.abs(self.hashCode()))
  val myGroupID:BigInt = BigInt(myNodeID)

  val listOfGroupServiceActorRefs = new ListBuffer[ActorRef]()
  val listOfGroupIDs = new ListBuffer[BigInt]()


  def createGroups(): Unit = {
    //create a groupMemberShipList and add local groupService actor, then write to KVStore
    val groupMemberShipList = new ListBuffer[ActorRef]()
    groupMemberShipList.append(self)
    cellstore.directWrite(myGroupID, groupMemberShipList)
  }



  def receive() = {
    case Prime() =>
      createGroups()
    case Command() =>
      statistics(sender)
      command
    case View(e) =>
      endpoints = Some(e)
    case Message(groupID, msg) =>
      stats.multiCastReceived+=1

      var tempMembershipMultiCastList = cellstore.directRead(groupID).get
      var tempMembershipMultiCastListAsString = tempMembershipMultiCastList.toString()
      val hashedList = sha256Hash(tempMembershipMultiCastListAsString)

      if(hashedList==msg) stats.messageOrderSame +=1
      else stats.messageOrderChanged +=1

      if(!tempMembershipMultiCastList.contains(self)){
        stats.leftGroupReceived +=1
      }

  }

  private def command() = {
    val sample = generator.nextInt(100)
    if (sample <= 75 && sample >=25) {
      joinGroup
    } else if (sample <25){
      leaveGroup
    } else if (sample > 75){
      MultiCast
    }

  }

  private def statistics(master: ActorRef) = {
    stats.messages += 1
    if (stats.messages >= burstSize) {
      master ! BurstAck(myNodeID, stats)
      stats = new Stats
    }
  }


  private def joinGroup() = {

    stats.joined +=1

    val randomID: Int = generator.nextInt(10)
    val randomGroupID = BigInt(randomID)

    // add a local weighting here for randomInt between 0-7, and else 8-9 (0-7 will have more members than 8-9)

    try{
      var tempMemberShipList= cellstore.directRead(randomGroupID).get
      if(!tempMemberShipList.contains(self)){
        tempMemberShipList += self
        cellstore.directWrite(myGroupID, tempMemberShipList)
      }
    }catch{
      case e:Exception => stats.misses +=1
    }

  }

  private def leaveGroup() = {

    val randomID: Int = generator.nextInt(10)
    val randomGroupID = BigInt(randomID)


    try{
      var tempMemberShipLeaveList = cellstore.directRead(randomGroupID).get
      if(tempMemberShipLeaveList.contains(self)) {
        tempMemberShipLeaveList -= self
        cellstore.directWrite(myGroupID, tempMemberShipLeaveList)
        stats.left += 1
      }
    } catch {
      case e:Exception => stats.misses +=1
    }

  }

  def sha256Hash(text: String) : String = String.format("%064x", new java.math.BigInteger(1, java.security.MessageDigest.getInstance("SHA-256").digest(text.getBytes("UTF-8"))))


  private def MultiCast(): Unit = {
    // add finding of random group the member is a part of
    val randomID: Int = generator.nextInt(10)
    val randomGroupID = BigInt(randomID)


    try{
      var tempMembershipMultiCastList = cellstore.directRead(randomGroupID).get
      var tempMembershipMultiCastListAsString = tempMembershipMultiCastList.toString()

      if (tempMembershipMultiCastList.contains((self))) {
        for (member <- tempMembershipMultiCastList) {

          var message = ("What's up Group from : " + member.path.name)
          var hashedList = sha256Hash(tempMembershipMultiCastListAsString)

          //send member actorRef along for the ride and check if in groupID, if not += lateReceive

          // how to have the client send the messages and not the member [ groupServer actorRefs]


          member ! Message(randomGroupID, hashedList)

          stats.multiCastSent += 1
        }
      }
    } catch{
      case e: Exception => stats.notInRandomGroup += 1
    }
  }


  private def read(key: BigInt): Option[GenericCell] = {
    val result = cellstore.read(key)
    if (result.isEmpty) None else
      Some(result.get.asInstanceOf[GenericCell])
  }

  private def write(key: BigInt, value: ListBuffer[ActorRef], dirtyset: AnyMap): Option[GenericCell] = {
    val coercedMap: AnyMap = dirtyset.asInstanceOf[AnyMap]
    val result = cellstore.write(key, value, coercedMap)
    if (result.isEmpty) None else
      Some(result.get.asInstanceOf[GenericCell])
  }

  private def directRead(key: BigInt): Option[GenericCell] = {
    val result = cellstore.directRead(key)
    if (result.isEmpty) None else
      Some(result.get.asInstanceOf[GenericCell])
  }

  private def directWrite(key: BigInt, value: ListBuffer[ActorRef]): Option[GenericCell] = {
    val result = cellstore.directWrite(key, value)
    if (result.isEmpty) None else
      Some(result.get.asInstanceOf[GenericCell])
  }

  private def push(dirtyset: AnyMap) = {
    cellstore.push(dirtyset)
  }
}

object GroupServer {
  def props(myNodeID: Int, numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int): Props = {
    Props(classOf[GroupServer], myNodeID, numNodes, storeServers, burstSize)
  }
}
