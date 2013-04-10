import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;


public class Search {

	public static WhitespaceAnalyzer analyzer;
	public static Directory index;
	public static IndexReader reader;
	public static IndexSearcher searcher;

	public static void main(final String args[]) {
		// Analyzer with no stopwords
		final File file = new File("/path/to/file");
		analyzer = new WhitespaceAnalyzer(Version.LUCENE_42);
		index = null;
		try {
			index = FSDirectory.open(file);
			reader = DirectoryReader.open(index);
			searcher = new IndexSearcher(reader);
		} catch (final IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		promptSearchType();
	}

	public static void promptSearchType() {
		System.out.println("Would you like to [lookup] or [search]?");
		final Scanner input = new Scanner(System.in);
		final String searchType = input.nextLine();
		if(searchType.equals("lookup")) {
			getLookupId();
		} else if (searchType.equalsIgnoreCase("search")) {
			getSearchTerm();
		} else {
			System.out.println("Not a valid input");
			promptSearchType();
		}
	}

	public static void getLookupId() {
		System.out.println("Enter a lookup ID:");
		final Scanner input = new Scanner(System.in);

		final String searchTerm = input.nextLine();
		lookup(searchTerm);

		getLookupId();
	}

	public static void getSearchTerm() {
		System.out.println("Enter a search term:");
		final Scanner input = new Scanner(System.in);

		final String searchTerm = input.nextLine();
		search(searchTerm);

		getSearchTerm();
	}

	public static void lookup(final String id) {
		try {
			final long startTime = System.currentTimeMillis();
			final Query q = new QueryParser(Version.LUCENE_41, "id", analyzer).parse(id);

			final TopDocs results = searcher.search(q, 10);

			final long endTime = System.currentTimeMillis();
			final long executionTime = endTime - startTime;
			System.out.println("Found " + results.totalHits + " hits in "+executionTime+" ms");

			if(results.totalHits == 0) return;
			final ScoreDoc[] hits = results.scoreDocs;
			final int docId = hits[0].doc;
			final Document d = searcher.doc(docId);
			System.out.println("\n\t\t" + d.get("song") + "\n\t\t" + d.get("artist") + "\n\t\t Score:" + hits[0].score + "\n\t\t Rank:" + d.get("rank"));


		} catch (final ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (final IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void search(final String searchTerm) {
		search(searchTerm, 5, 0, false);
	}

	public static void search(final String searchTerm, int limit, final int page, final boolean allWild) {
		// No more than 25 results ever
		final int maxLimit = 25;
		if(limit > maxLimit) limit = maxLimit;

		try {
			final long startTime = System.currentTimeMillis();
			final BooleanQuery q = new BooleanQuery();
			final String[] searchTerms = searchTerm.split(" ");
			// Add all search terms to boolean query
			for(int i = 0; i < searchTerms.length; i++) {
				// Don't add terms less than length 2 - they make for bad query results
				String searchString = searchTerms[i];
				if(searchString.length() > 1) {
					if(!allWild && i == searchTerms.length - 1) {
						searchString += "*";
					} else if(allWild) {
						searchString += "*";
					}
					final QueryParser parser = new QueryParser(Version.LUCENE_42, "searchable_song_artist", analyzer);
					// Special case for one word search to prevent wildcard messing up scoring
					// Search terms under 3 can break the search engine with SCORING_BOOLEAN_QUERY_REWRITE set
					if(searchTerms.length == 1 && searchTerms[0].length() > 3) {
						parser.setMultiTermRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
					}
					parser.setDefaultOperator(QueryParser.AND_OPERATOR);
					final Query query = parser.parse(searchString);
					q.add(query, Occur.MUST);
				}
			}

			final int searchLimit = (page+1) * limit;
			final TopDocs results = searcher.search(q, searchLimit);

			final long endTime = System.currentTimeMillis();
			final long executionTime = endTime - startTime;
			System.out.println("Found " + results.totalHits + " hits in "+executionTime+" ms");

			final ScoreDoc[] hits = results.scoreDocs;
			final int offset = page*limit;
			final int count = Math.min(results.totalHits - offset, limit);

			// Check if page number is invalid
			if(results.totalHits + limit < page*limit) return;

			if(count == 0 && allWild == false) {
				search(searchTerm, searchLimit, page, true);
			}
			for(int i = 0; i < count; ++i) {
			    final int docId = hits[i+offset].doc;
			    final Document d = searcher.doc(docId);
			    System.out.println((i + 1) + ". " + d.get("song") + "\n\t\t" + d.get("artist") + "\n\t\t Score:" + hits[i+offset].score + "\n\t\t Rank:" + d.get("rank"));
			}

		} catch (final ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (final IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
