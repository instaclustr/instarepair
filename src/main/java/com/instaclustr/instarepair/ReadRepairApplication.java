package com.instaclustr.instarepair;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.NoSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Read repair application.
 * <p>
 * Repairs a Cassandra cluster by using read repairs. Supports the same options as nodetool repair where it makes sense
 * to. The main difference is it repairs all replicas and does not support limiting repair to certain nodes or data
 * centers.
 * <p>
 * The problem with standard repairs occur when there is large amounts of inconsistency as any differences in the merkle
 * tree requiring streaming replicas from all nodes involved which can lead to:
 * <ul>
 * <li>Running out of disk space due to sending multiple replicas</li>
 * <li>Lots of small sstables from all streaming sstable sections for the inconsistent token range</li>
 * <li>Compactions fall behind from all the sstables being streamed</li>
 * <li>High read latency from an increase of sstables per read</li>
 * <li>High number of sstables causes compactions to use excessive amounts of CPU sorting sstables into buckets</li>
 * </ul>
 * <p>
 * In our experience we have seen repairs lead to cluster outages. The aim of this application is to avoid these issues
 * by instead relying on the less intensive read repairs which in comparison just send a mutation with the correct
 * version of the row to nodes without it. Additional this application supports suspending and resuming the repair. It
 * can also handle nodes going down. This makes it more robust then even tools such as cassandra reaper.
 */
public class ReadRepairApplication {
    private static final Logger logger = LoggerFactory.getLogger(ReadRepairApplication.class);

    private static final InetAddress IPV4_LOOPBACK = Inet4Address.getLoopbackAddress();

    private static final int DEFAULT_PORT = 9042;

    private static final int DEFAULT_STEPS = 1;

    private static final int DEFAULT_MAX_RETRY_ATTEMPTS = 3;

    private static final int DEFAULT_RETRY_DELAY = 60000; // 1 minute.

    /**
     * Local strategy keyspaces are not replicated and can not be read repaired.
     */
    private static final String[] LOCAL_STRATEGY_KEYSPACES = {"system", "system_schema"};

    /**
     * Replicated system keyspaces.
     */
    private static final String[] SYSTEM_KEYSPACES = {"system_auth", "system_distributed", "system_traces"};

    private static final String USERNAME_OPTION = "u";
    private static final String PASSWORD_OPTION = "pw";
    private static final String STEPS_OPTION = "s";
    private static final String HOST_OPTION = "h";
    private static final String PORT_OPTION = "p";
    private static final String SSL_OPTION = "ssl";
    private static final String HELP_OPTION = "help";
    private static final String PRIMARY_OPTION = "pr";
    private static final String EXCLUDE_OPTION = "exclude";
    private static final String EXCLUDE_SYSTEM_OPTION = "nosys";
    private static final String FILE_OPTION = "f";
    private static final String FRESH_OPTION = "fresh";
    private static final String REPORT_OPTION = "report";
    private static final String THREAD_OPTION = "t";
    private static final String RETRY_OPTION = "r";
    private static final String RETRY_DELAY_OPTION = "d";

    private static final Options options = new Options();

    static {
        Option optHelp = new Option(HELP_OPTION, "Display help");
        options.addOption(optHelp);

        Option optFile = new Option(FILE_OPTION, "file", true, "File with repair state");
        optFile.setArgName("filename");
        options.addOption(optFile);

        Option optFresh = new Option(FRESH_OPTION, "Start a fresh repair");
        options.addOption(optFresh);

        Option optHost = new Option(HOST_OPTION, "host", true, "Host to connect to");
        optHost.setArgName("host");
        options.addOption(optHost);

        Option optPort = new Option(PORT_OPTION, "port", true, "Port to connect to");
        optPort.setArgName("port");
        options.addOption(optPort);

        Option optSsl = new Option(SSL_OPTION, "Enable SSL");
        options.addOption(optSsl);

        Option optUsername = new Option(USERNAME_OPTION, "username", true, "Username");
        optUsername.setArgName("username");
        options.addOption(optUsername);

        Option optPassword = new Option(PASSWORD_OPTION, "password", true, "Password");
        optPassword.setArgName("password");
        options.addOption(optPassword);

        Option optPrimary = new Option(PRIMARY_OPTION, "partitioner-range", false, "Perform partitioner range repair");
        options.addOption(optPrimary);

        Option optSteps = new Option(STEPS_OPTION, "steps", true, "Steps per token range");
        optSteps.setArgName("steps");
        options.addOption(optSteps);

        Option optExclude = new Option(EXCLUDE_OPTION, true, "Keyspaces (comma separated) to exclude");
        optExclude.setArgName("keyspaces");
        options.addOption(optExclude);

        Option optExcludeSystem = new Option(EXCLUDE_SYSTEM_OPTION, "exclude-system", false, "Exclude system keyspaces");
        options.addOption(optExcludeSystem);

        Option optReport = new Option(REPORT_OPTION, "Report state of repair and exit");
        options.addOption(optReport);

        Option optThreads = new Option(THREAD_OPTION, "threads", true, "Number of threads. Defaults to number of available processors.");
        optThreads.setArgName("threads");
        options.addOption(optThreads);

        Option optRetry = new Option(RETRY_OPTION, "retry", true, "Maximum number of times to retry when there are unavailable nodes");
        optRetry.setArgName("max_retry");
        options.addOption(optRetry);

        Option optRetryDelay = new Option(RETRY_DELAY_OPTION, "retry-delay", true, "Delay between retries when nodes are unavailable");
        optRetryDelay.setArgName("delay_ms");
        options.addOption(optRetryDelay);
    }

    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ic-repair [<keyspace> <tables>...]", "Perform read repair", options, null);
    }

    public static void main(String[] args) {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            logger.error("Command line parse error", e);
            System.exit(1);
        }

        if (cmd.hasOption(HELP_OPTION)) {
            printHelp();
            System.exit(0);
        }

        String filename = cmd.getOptionValue(FILE_OPTION, "repair.bin");
        File file = new File(filename);

        boolean fresh = cmd.hasOption(FRESH_OPTION);

        boolean resumeRepair = !fresh && file.exists();

        InetAddress address = IPV4_LOOPBACK;
        if (cmd.hasOption(HOST_OPTION)) {
            try {
                address = Inet4Address.getByName(cmd.getOptionValue(HOST_OPTION));
            } catch (UnknownHostException e) {
                logger.error("Unknown host", e);
                System.exit(1);
            }
        }

        int port = DEFAULT_PORT;
        if (cmd.hasOption(PORT_OPTION)) {
            try {
                port = Integer.parseInt(cmd.getOptionValue(PORT_OPTION));
            } catch (NumberFormatException e) {
                logger.error("Invalid port", e);
                System.exit(1);
            }
        }

        boolean primaryOnly = cmd.hasOption(PRIMARY_OPTION);
        if (resumeRepair && primaryOnly) {
            logger.info("Ignoring -pr option due to resuming repair");
        }

        int steps = DEFAULT_STEPS;
        if (cmd.hasOption(STEPS_OPTION)) {
            try {
                steps = Integer.parseInt(cmd.getOptionValue(STEPS_OPTION));
            } catch (NumberFormatException e) {
                logger.error("Invalid steps", e);
                System.exit(1);
            }
            if (resumeRepair) {
                logger.info("Ignoring steps option due to resuming repair");
            }
        }

        if (cmd.hasOption(USERNAME_OPTION) && !cmd.hasOption(PASSWORD_OPTION)) {
            logger.error("Must provide password with username!");
            System.exit(1);
        }
        if (cmd.hasOption(PASSWORD_OPTION) && !cmd.hasOption(USERNAME_OPTION)) {
            logger.error("Must provide username with password!");
            System.exit(1);
        }

        List<String> arguments = cmd.getArgList();
        String keyspaceName = null;
        List<String> tableNames = null;
        if (!arguments.isEmpty()) {
            keyspaceName = arguments.get(0);
            tableNames = arguments.subList(1, arguments.size());
        }
        if (resumeRepair && keyspaceName != null) {
            logger.info("Ignoring keyspace/table arguments due to resuming repair");
        }

        List<String> excludeList = new ArrayList<>();
        Collections.addAll(excludeList, LOCAL_STRATEGY_KEYSPACES);
        if (cmd.hasOption(EXCLUDE_SYSTEM_OPTION)) {
            Collections.addAll(excludeList, SYSTEM_KEYSPACES);
            if (resumeRepair) {
                logger.info("Ignoring exclude-system option due to resuming repair");
            }
        }
        if (cmd.hasOption(EXCLUDE_OPTION)) {
            Collections.addAll(excludeList, cmd.getOptionValue(EXCLUDE_OPTION).split(","));
            if (resumeRepair) {
                logger.info("Ignoring exclude option due to resuming repair");
            }
        }

        if (!excludeList.isEmpty() && keyspaceName != null) {
            logger.info("Ignoring exclude {} since repairing keyspace {}", excludeList, keyspaceName);
        }

        AuthProvider authProvider = null;
        if (cmd.hasOption(PASSWORD_OPTION) && cmd.hasOption(USERNAME_OPTION)) {
            String username = cmd.getOptionValue(USERNAME_OPTION);
            String password = cmd.getOptionValue(PASSWORD_OPTION);
            authProvider = new PlainTextAuthProvider(username, password);
        }

        boolean sslEnabled = cmd.hasOption(SSL_OPTION);

        int threads = Runtime.getRuntime().availableProcessors();
        if (cmd.hasOption(THREAD_OPTION)) {
            try {
                threads = Integer.parseInt(cmd.getOptionValue(THREAD_OPTION));
            } catch (NumberFormatException e) {
                logger.error("Invalid threads", e);
                System.exit(1);
            }
        }

        int maxRetryAttempts = DEFAULT_MAX_RETRY_ATTEMPTS;
        if (cmd.hasOption(RETRY_OPTION)) {
            try {
                maxRetryAttempts = Integer.parseInt(cmd.getOptionValue(RETRY_OPTION));
            } catch (NumberFormatException e) {
                logger.error("Invalid max_retry", e);
                System.exit(1);
            }
        }

        int retryDelayMilliseconds = DEFAULT_RETRY_DELAY;
        if (cmd.hasOption(RETRY_DELAY_OPTION)) {
            try {
                retryDelayMilliseconds = Integer.parseInt(cmd.getOptionValue(RETRY_DELAY_OPTION));
            } catch (NumberFormatException e) {
                logger.error("Invalid delay_ms", e);
                System.exit(1);
            }
        }

        logger.info("Starting read repair application...");
        try (final Cluster cluster = connect(address, port, authProvider, sslEnabled)) {
            final Metadata metadata = cluster.getMetadata();

            if (!metadata.getPartitioner().equals("org.apache.cassandra.dht.Murmur3Partitioner")) {
                logger.error("Only Murmur3 partitioner is supported!");
                cluster.close();
                System.exit(1);
            }

            Host _host = null;
            // Get the host by address.
            for (final Host host : metadata.getAllHosts()) {
                if (host.getAddress().equals(address)) {
                    _host = host;
                    break;
                }
            }
            if (_host == null) {
                logger.error("Unable to find host {}", address);
                System.exit(1);
            }
            final Host host = _host;

            Session session = cluster.connect();

            RepairTask _repairTask = null;

            if (resumeRepair) {
                // Resume the existing repair.
                try (FileInputStream fis = new FileInputStream(file);
                     ObjectInputStream in = new ObjectInputStream(fis)) {
                    _repairTask = new RepairTask(maxRetryAttempts, retryDelayMilliseconds, host, metadata, session, in, threads);
                    logger.info("Resuming repair...");
                } catch (Exception e) {
                    logger.error("Exception while loading repair state", e);
                    System.exit(1);
                }
            } else if (keyspaceName != null) {
                KeyspaceMetadata keyspace = metadata.getKeyspace(keyspaceName);
                if (keyspace == null) {
                    logger.error("Unable to find keyspace {}", keyspaceName);
                    System.exit(1);
                }
                if (tableNames.isEmpty()) {
                    // Only keyspace specified, repair all tables in the keyspace.
                    Queue<KeyspaceMetadata> keyspaces = new LinkedList<>();
                    keyspaces.add(keyspace);
                    _repairTask = new RepairTask(maxRetryAttempts, retryDelayMilliseconds, host, metadata, session, primaryOnly, threads, steps, keyspaces);
                } else {
                    // Repair specified tables in the keyspace.
                    Queue<TableMetadata> tables = new LinkedList<>();
                    for (String tableName : tableNames) {
                        TableMetadata table = keyspace.getTable(tableName);
                        if (table == null) {
                            logger.error("Unable to find table {}", tableName);
                            System.exit(1);
                        }
                        tables.add(table);
                    }
                    _repairTask = new RepairTask(maxRetryAttempts, retryDelayMilliseconds, host, metadata, session, primaryOnly, threads, steps, keyspace, tables);
                }
            } else {
                // Repair all non-excluded keyspaces.
                List<KeyspaceMetadata> keyspaces = metadata.getKeyspaces();
                if (!excludeList.isEmpty()) {
                    keyspaces = keyspaces.stream().filter(ks -> !excludeList.contains(ks.getName())).collect(Collectors.toList());
                }
                _repairTask = new RepairTask(maxRetryAttempts, retryDelayMilliseconds, host, metadata, session, primaryOnly, threads, steps, new LinkedList<>(keyspaces));
                logger.info("Starting repair...");
            }

            final RepairTask repairTask = _repairTask;
            if (cmd.hasOption(REPORT_OPTION)) {
                System.out.println("Repair state:");
                System.out.println(repairTask);
                System.exit(0);
            }
            logger.info("Loading {}", repairTask);

            // Execute repairTask with future so shutdown hook can wait for clean shutdown on interrupt.
            Future<Boolean> repairFuture = Executors.newSingleThreadExecutor().submit(repairTask);

            Thread shutdownHook = new Thread(() -> {
                logger.info("Shutting down...");
                repairTask.shutdown();
                if (Futures.getUnchecked(repairFuture)) {
                    if (file.exists()) {
                        if (!file.delete()) {
                            logger.error("Failed to delete " + file.getAbsolutePath());
                        }
                    }
                } else {
                    logger.info("Saving {}", repairTask);
                    try (FileOutputStream fos = new FileOutputStream(file);
                         ObjectOutputStream out = new ObjectOutputStream(fos)) {
                        repairTask.writeObject(out);
                        logger.info("Repair suspended");
                    } catch (Exception e) {
                        logger.error("Exception while saving repair", e);
                    }
                }

                // Close Cassandra session.
                session.close();
                logger.info("Shutdown");
            });
            Runtime.getRuntime().addShutdownHook(shutdownHook);

            boolean repaired = Futures.getUnchecked(repairFuture);
            if (repaired) {
                logger.info("Repair successful");
            } else {
                logger.info("Repair failed");
            }
            System.exit(repaired ? 0 : 1);

        } catch (Throwable t) {
            logger.error("Application error", t);
        }
    }

    /**
     * Connect to Cassandra cluster limited to the specified node address.
     * <p>
     * Consistency level is set to ALL since we want all replicas involved in the reads. Retry and speculative execution
     * are disabled since RepairTask has its own retry mechanisms.
     *
     * @param address      The network address of the Cassandra node to repair
     * @param port         The port for Cassandra TCP connection
     * @param authProvider Authentication provider for connection authentication
     * @param sslEnabled   Use SSL for Cassandra connection.
     * @return Cassandra Cluster
     */
    private static Cluster connect(InetAddress address, int port, AuthProvider authProvider, boolean sslEnabled) {
        final Cluster.Builder clusterBuilder = Cluster.builder()
                .addContactPoints(address)
                .withPort(port)
                .withQueryOptions(new QueryOptions()
                        .setConsistencyLevel(ConsistencyLevel.ALL)
                )
                .withRetryPolicy(new NoRetryPolicy())
                .withSpeculativeExecutionPolicy(NoSpeculativeExecutionPolicy.INSTANCE)
                .withLoadBalancingPolicy(new WhiteListPolicy(DCAwareRoundRobinPolicy.builder().build(), ImmutableList.of(new InetSocketAddress(address, port))));
        if (authProvider != null) {
            clusterBuilder.withAuthProvider(authProvider);
        }
        if (sslEnabled) {
            clusterBuilder.withSSL();
        }
        return clusterBuilder.build();
    }
}
