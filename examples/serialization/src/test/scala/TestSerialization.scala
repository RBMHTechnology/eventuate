import com.rbmhtechnology.eventuate.{VectorTime, Versioned}
import org.apache.commons.lang3.SerializationUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest._
import org.scalatest.Matchers._

@RunWith(classOf[JUnitRunner])
class TestSerialization extends WordSpec {
  "VectorTime" should {
    val desired = VectorTime("a" -> 1L, "b" -> 2L, "c" -> 3L)

    "manually serialize and deserialize" in {
      val byteArray: Array[Byte] = SerializationUtils.serialize(desired.value.asInstanceOf[Serializable])
      val map: Map[String, Long] = SerializationUtils.deserialize(byteArray)
      val actual = VectorTime(map.toSeq: _*)
      actual === desired
    }

    "implicitly serialize and deserialize" in {
      import sapi._
      val byteArray: Array[Byte] = desired.serialize
      val actual: VectorTime = RichVectorTime.deserialize(byteArray)
      actual === desired
    }
  }

  "Versioned" should {
    val vectorTime = VectorTime("a" -> 1L, "b" -> 2L, "c" -> 3L)
    val systemTimeStamp = System.currentTimeMillis
    val creator = "The Software God"
    val desired = Versioned("value", vectorTime, systemTimeStamp, creator)

    "manually serialize and deserialize" in {
      val byteArray: Array[Byte] = SerializationUtils.serialize(desired.asInstanceOf[Serializable])
      val actual: Versioned[String] = SerializationUtils.deserialize(byteArray)
      actual === desired
    }

    "implicitly serialize and deserialize" in {
      import sapi._
      val byteArray: Array[Byte] = desired.serialize
      val actual: Versioned[String] = RichVersioned.deserialize(byteArray)
      actual === desired
    }
  }
}
