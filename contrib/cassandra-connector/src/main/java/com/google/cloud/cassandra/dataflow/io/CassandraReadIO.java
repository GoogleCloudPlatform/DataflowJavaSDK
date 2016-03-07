package com.google.cloud.cassandra.dataflow.io;
import java.util.ArrayList;
import java.util.List;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;

/**
 * Class to create read operation from Cassandra using entities in a
 * Google Dataflow pipeline.
 * 
 * To read from a cassandra database table : 
 * String query = QueryBuilder.select().all()
				.from(Utils.KEYSPACE, "emp_info").toString();
 * CassandraReadIO.Read.Bound<String> bound = new Read.Bound<String>(Utils.HOSTS, 
 * 						Utils.KEYSPACE, Utils.PORT, EmployeeDetails.class,query);
 * CassandraReadIO.Read.CassandraReadOperation<String> cassandraRead = new Read.
 * 						CassandraReadOperation<>(bound);
 * PipelineOptions options = PipelineOptionsFactory.create();
 * Pipeline p = Pipeline.create(options);
 * PCollection pcollection = p.apply(Create.of(cassandraRead
 *				.apply()));
 *				p.run();		
 *
 */
public class CassandraReadIO {

	/*
	 * This class connects to cassandra cluster and performs read operation.
	 */
	public static class Read {

		public static class Bound<T> {

			private static final long serialVersionUID = 0;

			private final String[] _hosts;
			private final String _keyspace;
			private final int _port;
			private final Class _entityName;
			private final String _query;

			public Bound(String[] hosts, String keyspace, int port, Class entityName,
					String query) {
				_hosts = hosts;
				_keyspace = keyspace;
				_port = port;
				_entityName = entityName;
				_query = query;
			}

			public String[] getHosts() {
				return _hosts;
			}

			public String getKeyspace() {
				return _keyspace;
			}

			public int getPort() {
				return _port;
			}

			public Class getEntityName() {
				return _entityName;
			}

			public String getQuery() {
				return _query;
			}
		}

		/**
	     * This class defines configuration for a cassandra connection, a table, entity name and query.
	     * It also retrives a resultset and transform to List of specific entity type 
	     */
		public static class CassandraReadOperation<T> implements
				java.io.Serializable {

			private static final long serialVersionUID = 0;

			private final String[] _hosts;
			private final int _port;
			private final String _keyspace;
			private final Class _entityName;
			private final String _query;

			private transient Cluster _cluster;
			private transient Session _session;
			private transient MappingManager _manager;

			private synchronized Cluster getCluster() {
				if (_cluster == null) {
					_cluster = Cluster.builder().addContactPoints(_hosts)
					 .withPort(_port) .withoutMetrics()
							.withoutJMXReporting().build();
				}

				return _cluster;
			}

			private synchronized Session getSession() {

				if (_session == null) {
					Cluster cluster = getCluster();
					_session = cluster.connect(_keyspace);
				}

				return _session;
			}

			private synchronized MappingManager getManager() {
				if (_manager == null) {
					Session session = getSession();
					_manager = new MappingManager(_session);
				}
				return _manager;
			}

			public CassandraReadOperation(Bound<T> bound) {
				_hosts = bound.getHosts();
				_port = bound.getPort();
				_keyspace = bound.getKeyspace();
				_entityName = bound.getEntityName();
				_query = bound.getQuery();
			}

			public CassandraRead<T> createReader() {
				return new CassandraRead<T>(this, getManager());
			}

			public void finalize() {
				getSession().close();
				getCluster().close();
			}

			/**
			 * Method retrieve rows from cassandra and transform to List
			 * of specific entity type 
			 */ 
			public List<T> apply() {
				List<T> lstEmp = new ArrayList<T>();

				ResultSet rs = getSession().execute(_query);
				Result r = createReader().read(rs, _entityName);
				while (r.iterator().hasNext()) {
					lstEmp.add((T) r.iterator().next());
				}
				return lstEmp;
			}
		}

		/*
		 * This class converts the ResultSet to the entity map
		 */
		private static class CassandraRead<T> {
			private final CassandraReadOperation _op;
			private final MappingManager _manager;
			private Mapper<T> _mapper;

			public CassandraRead(CassandraReadOperation op,
					MappingManager manager) {
				_op = op;
				_manager = manager;
			}
			
			/**
			 * method will map the entity with resultset and returns map of specific entity type
			 */ 
			@SuppressWarnings("unchecked")
			public Result<T> read(ResultSet rs, Class entityName) {
				if(_mapper==null){
					_mapper = (Mapper<T>) _manager.mapper(entityName);
				}
				return _mapper.map(rs);
			}
		}
	}

}
