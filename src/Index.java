import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;


import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class Index {

	public static void main(final String[] args) {
		final long startTime = new Date().getTime();
		// Analyzer with no stopwords
		final File file = new File("/path/to/file");
		final StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_43, new CharArraySet(Version.LUCENE_43, new ArrayList<>(), true));
		// Delete all old indexes
		final String[] myFiles = file.list();
		for (int i = 0; i < myFiles.length; i++) {
			final File subFile = new File(file, myFiles[i]);
			subFile.delete();
		}
		final long deleteTime = new Date().getTime();
		System.out.println("Time to delete old index: " + (deleteTime - startTime));
		Directory index = null;
		try {
			index = FSDirectory.open(file);
		}
		catch (final IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		final LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
		final IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43, analyzer);
		config.setMergePolicy(mergePolicy);

		int count = 0;
		Connection connection = null;
		long sqlTime = 0;
		try {
			System.out.println("Connecting to SQL...");
			// Make connection
			final String url = "url";
		    final String user = "root";
		    final String password = "password";

		    connection = DriverManager.getConnection(url, user, password);
			final String statement = "SELECT * FROM table;";
			final Statement stmt = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
					java.sql.ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(100000);
			connection.setAutoCommit(false);
			final ResultSet results = stmt.executeQuery(statement);
			sqlTime = new Date().getTime();
			System.out.println("Time to execute SQL: " + (sqlTime - deleteTime));
			System.out.println("Building index...");
			try {
				final IndexWriter w = new IndexWriter(index, config);
				// Create fields once only, to avoid GC				
				final Field idField = new Field("id", "", idType());
				final StoredField songField = new StoredField("song", "");
				final StoredField artistField = new StoredField("artist", "");
				//				final NumericDocValuesField rankField = new NumericDocValuesField("rank", 0);
				final Field searchField = new Field("searchable_song_artist", "", indexType());

				while (results.next()) {
					count++;

					final Document document = new Document();
					idField.setStringValue(String.valueOf(results.getInt("id")));
					songField.setStringValue(results.getString("song"));
					artistField.setStringValue(results.getString("artist"));
					//					rankField.setLongValue(results.getInt("rank"));

					document.add(idField);
					document.add(songField);
					document.add(artistField);
					//					document.add(rankField);
					searchField.setStringValue(results.getString("searchable_song") + " " + results.getString("searchable_artist"));
					searchField.setBoost(results.getInt("rank"));
					document.add(searchField);
					w.addDocument(document);

				}
				w.forceMerge(1);
				w.close();
			}
			catch (final IOException e) {
				e.printStackTrace();
			}
		}
		catch (final SQLException e) {
			e.printStackTrace();
		}
		finally {
			if (connection != null) {
				try {
					connection.close();
				}
				catch (final SQLException e) {
					e.printStackTrace();
				}
			}
			System.out.println("Index built: " + count + " entries");
		}
		final long endTime = new Date().getTime();
		System.out.println("Time to build index: " + (endTime - sqlTime));
		final int totalMilli = (int)(endTime - startTime);
		final int minutes = totalMilli / 1000 / 60;
		final int seconds = (totalMilli / 1000) % 60;
		final int milli = totalMilli % 1000;
		System.out.println("Total time: " + minutes + "m " + seconds + "s " + milli + "ms ");

	}

	private static FieldType idType() {
		final FieldType idType = new FieldType();
		idType.setIndexed(true);
		idType.setStored(true);
		return idType;
	}

	private static FieldType indexType() {
		final FieldType indexType = new FieldType();
		indexType.setIndexed(true);
		indexType.setStored(false);
		indexType.setTokenized(true);
		return indexType;
	}

}
