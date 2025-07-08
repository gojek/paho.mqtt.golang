package mqtt

// ClientLogger provides a structured way to log messages for different client actions.
// It contains loggers for ERROR, CRITICAL, WARN, and DEBUG levels, each associated with a specific client ID.
// ClientLogger is an interface for structured client loggers.
type ClientLogger interface {
	Error() Logger
	Critical() Logger
	Warn() Logger
	Debug() Logger
}

// DefaultClientLogger implements ClientLogger.
type DefaultClientLogger struct {
	clientID       string
	errorLogger    Logger
	criticalLogger Logger
	warnLogger     Logger
	debugLogger    Logger
}

func (l *DefaultClientLogger) Error() Logger {
	return l.errorLogger
}

func (l *DefaultClientLogger) Critical() Logger {
	return l.criticalLogger
}

func (l *DefaultClientLogger) Warn() Logger {
	return l.warnLogger
}

func (l *DefaultClientLogger) Debug() Logger {
	return l.debugLogger
}

// prefixLogger wraps a Logger to prefix messages with clientID.
type prefixLogger struct {
	clientID string
	logger   Logger
}

func (pl *prefixLogger) Println(v ...interface{}) {
	v = append([]interface{}{"[" + pl.clientID + "]"}, v...)
	pl.logger.Println(v...)
}

func (pl *prefixLogger) Printf(format string, v ...interface{}) {
	format = "[" + pl.clientID + "] " + format
	pl.logger.Printf(format, v...)
}

// NewClientLogger creates a new DefaultClientLogger with the provided clientID and loggers for different levels.
// If any of the loggers are nil, they will default to NOOPLogger.
func NewClientLogger(clientID string, errorLogger, criticalLogger, warnLogger, debugLogger Logger) ClientLogger {
	if errorLogger == nil {
		errorLogger = ERROR
	}
	if criticalLogger == nil {
		criticalLogger = CRITICAL
	}
	if warnLogger == nil {
		warnLogger = WARN
	}
	if debugLogger == nil {
		debugLogger = DEBUG
	}
	return &DefaultClientLogger{
		clientID:       clientID,
		errorLogger:    &prefixLogger{clientID: clientID, logger: errorLogger},
		criticalLogger: &prefixLogger{clientID: clientID, logger: criticalLogger},
		warnLogger:     &prefixLogger{clientID: clientID, logger: warnLogger},
		debugLogger:    &prefixLogger{clientID: clientID, logger: debugLogger},
	}
}
