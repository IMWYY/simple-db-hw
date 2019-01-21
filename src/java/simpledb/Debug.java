package simpledb;

/**
 * Debug is a utility class that wraps println statements and allows
 * more or less command line output to be turned on.
 * <p>
 * Change the value of the DEBUG_LEVEL constant using a system property:
 * simpledb.Debug. For example, on the command line, use -Dsimpledb.Debug=x,
 * or simply -Dsimpledb.Debug to enable it at level 0.
 * The log(level, message, ...) method will print to standard output if the
 * level number is less than or equal to the currently set DEBUG_LEVEL.
 */

public class Debug {

	/**
	 * 0: ERROR
	 * 1: INFO
	 * 2: DEBUG
	 */
	public static final int LEVEL_ERROR = 0;
	public static final int LEVEL_INFO = 1;
	public static final int LEVEL_DEBUG = 2;

	private static final int CUR_LOG_LEVEL = 2;
	private static final boolean LOG_SWITCH_ON = true;

	/**
	 * Log message if the log DEBUG_LEVEL >= level. Uses printf.
	 */
	public static void log(int level, String message, Object... args) {
		if (isEnabled(level)) {
			System.out.printf(getLogLevelStr(level) + message, args);
			System.out.println();
		}
	}

	/**
	 * @return true if level is being logged.
	 */
	public static boolean isEnabled(int level) {
		return level <= CUR_LOG_LEVEL;
	}

	/**
	 * @return true if the default level is being logged.
	 */
	public static boolean isEnabled() {
		return LOG_SWITCH_ON;
	}

	/**
	 * Logs message at the default log level.
	 */
	public static void log(String message, Object... args) {
		log(LEVEL_INFO, message, args);
	}

	private static String getLogLevelStr(int level) {
		switch (level) {
		case LEVEL_ERROR:
			return "[ERROR] ";
		case LEVEL_INFO:
			return "[INFO]  ";
		case LEVEL_DEBUG:
			return "[DEBUG] ";
		default:
			return "";
		}
	}

}
