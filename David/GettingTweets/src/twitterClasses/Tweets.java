package twitterClasses;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class Tweets
{
	public static void main(String[] args) 
	{
		try
		{
			getTweets();
		}
		catch(Exception e)
		{
			System.out.println("Error occurred");
			e.printStackTrace();
		}
	}
	public static void getTweets() throws TwitterException, FileNotFoundException
	{
		ConfigurationBuilder cb = new ConfigurationBuilder();
	    cb.setDebugEnabled(true)
	            .setOAuthConsumerKey("00qjvCqa0iU8CS9o6gRJ1lJLc")
	            .setOAuthConsumerSecret("5SrrZxXiOkRgwiNysy5G9wzW7L6N7NSYQyT5ZS2XLd6PRrxiPW")
	            .setOAuthAccessToken("4850382254-hmgYUKnQlGXakjSdPxt2dbMjy1ovgWgoK9HrjjS")
	            .setOAuthAccessTokenSecret("5II87hOGOh4enNlbNmz11Qx0w9jgb8sAoOAD3xlCj2UT6");

	    StatusListener listener = new StatusListener()
	    {
	        public void onStatus(Status status){ System.out.println(status.getUser().getName() + " : " + status.getText()); }
	        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
	        public void onException(Exception ex){ ex.printStackTrace(); }
			@Override
			public void onScrubGeo(long arg0, long arg1) {}
			@Override
			public void onStallWarning(StallWarning arg0) {}
	    };
	    
	    TwitterStreamFactory tsf= new TwitterStreamFactory(cb.build());
	    TwitterStream twitterStream = tsf.getInstance();
	    twitterStream.addListener(listener);
	        
	    // sample() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
	    twitterStream.sample("en");
	    
	    
	    // filters based on word, location and language
	    //FilterQuery tweetFilterQuery = new FilterQuery(); 
	    //tweetFilterQuery.track(new String[]{"cat"});
	    //tweetFilterQuery.locations(new double[][]{new double[]{42.923447,-81.338928}, new double[]{43.057550,-81.124695}}); // coordinates for London, ON
	    //tweetFilterQuery.language(new String[]{"en"});
	    //twitterStream.filter(tweetFilterQuery);
	    
	    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	    PrintStream out = new PrintStream(new FileOutputStream("collectedTweets_englishOnly.txt"));
	    System.setOut(out);
	}
}