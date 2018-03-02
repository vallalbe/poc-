package com.pari.poc

import twitter4j.conf.ConfigurationBuilder
import twitter4j.{Trends, TwitterFactory}

object ScalaTwitterClientExample {
  def main(args: Array[String]) {

    // (1) config work to create a twitter object
    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey("eLfAB87h9s9Knw1MQQuT1NvuY")
      .setOAuthConsumerSecret("eUO2XYafc7tGWzA5ZKwfikVm5qegNZpyrMzDPvPeRHsT81hnL8")
      .setOAuthAccessToken("331719639-1k9jxXdkU3eCo9uy4uNisdhVY5VhkyGbwGUIX5FF")
      .setOAuthAccessTokenSecret("dteqiw3GIE1ysn4uuUzvd61qxx4qvtQRYmTYTPFYK4Swq");
    val tf = new TwitterFactory(cb.build())
    val twitter = tf.getInstance()

    // (2) use the twitter object to get your friend's timeline
    val trends: Trends = twitter.getPlaceTrends(1)
    var count = 0
    for (t <- trends.getTrends()) {
      if (count < 10) {
        println(t.getName())
        count + 1
      }

    }
  }
}
