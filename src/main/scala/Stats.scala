class Stats {
  var messages: Int = 0
  var allocated: Int = 0
  var checks: Int = 0
  var touches: Int = 0
  var misses: Int = 0
  var errors: Int = 0

  // added these to test command
  var joined: Int = 0;
  var left: Int = 0;
  var leftGroupReceived: Int = 0;
  var multiCastReceived: Int = 0
  var multiCastSent: Int = 0
  var notInRandomGroup: Int =0

  var messageOrderSame: Int = 0
  var messageOrderChanged: Int = 0

  def += (right: Stats): Stats = {
    messages += right.messages
    allocated += right.allocated
    checks += right.checks
    touches += right.touches
    misses += right.misses
    errors += right.errors

    joined += right.joined
    left += right.left
    leftGroupReceived += right.leftGroupReceived
    multiCastReceived += right.multiCastReceived
    multiCastSent += right.multiCastSent
    notInRandomGroup += right.notInRandomGroup
    messageOrderSame += right.messageOrderSame
    messageOrderChanged += right.messageOrderChanged

    this
  }

  override def toString(): String = {
    //s"Stats msgs=$messages alloc=$allocated checks=$checks touches=$touches miss=$misses err=$errors"
    s"Stats msgs=$messages joined=$joined left=$left multiCastReceived=$multiCastReceived multiCastSent=$multiCastSent messageOrderSame=$messageOrderSame messageOrderChanged=$messageOrderChanged leftGroupReceived=$leftGroupReceived "
  }
}
