import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;


public class Index {

	public static void main(final String [] args) {
		// Analyzer with no stopwords
		final StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_41, new CharArraySet(Version.LUCENE_41, 0, true));
		final File file = new File("/path/to/file");
		// Delete all old indexes
		final String[] myFiles = file.list();
		for(int i = 0; i < myFiles.length; i++) {
			final File subFile = new File(file, myFiles[i]);
			subFile.delete();
		}
		Directory index = null;
		try {
			index = FSDirectory.open(file);
		} catch (final IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		final LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
		final IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_41, analyzer);
		config.setMergePolicy(mergePolicy);

		Connection connection = null;
		try {
			Logger.getAnonymousLogger().log(Level.SEVERE, "Starting SQL");
			// Make connection
			final String url = "url";
		    final String user = "root";
		    final String password = "password";

		    connection = DriverManager.getConnection(url, user, password);
			final String statement = "SELECT * FROM table;";
			final Statement stmt = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
		              java.sql.ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(Integer.MIN_VALUE);
			final ResultSet results = stmt.executeQuery(statement);
			try {
				final IndexWriter w = new IndexWriter(index, config);
				while(results.next()) {
					final Document document = new Document();
					document.add(new StoredField("id", results.getString("mbid").substring(29)));
					document.add(new StoredField("song", results.getString("song")));
					document.add(new StoredField("artist", results.getString("artist")));
					document.add(new IntDocValuesField("rank", results.getInt("rank")));
					// TODO: See if it is possible to use correct filters with Lucene so we don't need extra fields
					final Field searchField = new Field("searchable_song_artist", results.getString("searchable_song")+" "+results.getString("searchable_artist"), indexType());
					searchField.setBoost(results.getInt("rank"));
					document.add(searchField);
					w.addDocument(document);
				}
				w.close();
			} catch (final IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (final SQLException ignore) {
			ignore.printStackTrace();
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (final SQLException ignore) {
					ignore.printStackTrace();
				}
			}
			Logger.getAnonymousLogger().log(Level.SEVERE, "Done with SQL");
		}

	}

	private static FieldType indexType() {
        final FieldType indexType = new FieldType();
        indexType.setIndexed(true);
        indexType.setStored(false);
        indexType.setTokenized(true);
        return indexType;
	}

}
