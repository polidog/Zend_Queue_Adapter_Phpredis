<?php
/**
 * @see Zend_Queue_Adapter_AdapterAbstract
 */
require_once 'Zend/Queue/Adapter/AdapterAbstract.php';

/**
 * Zend_Queue_Adapter_PhpRedis
 * 
 * @author polidog
 * @version 0.1
 */
class Zend_Queue_Adapter_Phpredis extends Zend_Queue_Adapter_AdapterAbstract
{
    
    const DEFAULT_HOST    = '127.0.0.1';
    const DEFAULT_PORT    = 6379;
    const DEFAULT_TIMEOUT = 2.5; 
    const DEFAULT_KEY_PREFIX  = 'Zend_Queue';
    
    /**
     * @var Redis
     */
    protected $_cache = null;
    
    /**
     * @var string
     */
    protected $_host = null;
    
    /**
     * @var integer
     */
    protected $_port = null;
    
    /**
     * @ver string
     */
    protected $_keyPrefix = null;
    
    
    /**
     * COnstructor
     * 
     * @param array|Zend_Config $options
     * @param null|Zend_Queue $queue
     */
    public function __construct($options, Zend_Queue $queue = null)
    {
        if (!extension_loaded('redis')) {
            require_once 'Zend/Queue/Exception.php';
            throw new Zend_Queue_Exception('Memcache extension does not appear to be loaded');
        }
        
        parent::__construct($options, $queue);
        
        $options = $this->_options['driverOptions'];
        
        
        if (!array_key_exists('host', $options)) {
            $options['host'] = self::DEFAULT_HOST;
        }
        if (!array_key_exists('port', $options)) {
            $options['port'] = self::DEFAULT_PORT;
        }
        if (!array_key_exists('prefix', $options)) {
            $prefix = self::DEFAULT_KEY_PREFIX;
        }
        
        $this->_cache = new Redis();    
        $result = $this->_cache->connect($options['host'], $options['port']);
        
        if ( $result == false ) {
            require_once 'Zend/Queue/Exception.php';
            throw new Zend_Queue_Exception('Could not connect to Redis');
        }
//        $this->_cache->setOption(Redis::OPT_SERIALIZER,Redis::SERIALIZER_PHP);
        
        $this->_host = $options['host'];
        $this->_port = (int)$options['port'];
        $this->_keyPrefix = $prefix;
    }
    
    
    /**
     * Destructor
     */
    public function __destruct() {
        if ($this->_cache instanceof Memcache) {
            $this->_cache->close();
        }
    }
    
    /********************************************************************
     * Queue management functions
     *********************************************************************/
    
    /**
     * Does a queue already exist?
     * 
     * @param  string $name
     * @return boolean
     */
    public function isExists($name) {
        if (empty($this->_queues)) {
            $this->getQueues();
        }
        return in_array($name, $this->_queues);
    }
    
    /**
     * Create a new queue
     *
     * @param  string  $name    queue name
     * @param  integer $timeout default visibility timeout
     * @return boolean
     * @throws Zend_Queue_Exception
     */
    public function create($name, $timeout=null) {
        $key = $this->_getKey($name);
//        $this->_cache->setTimeout($key,$timeout);    
        $this->_queues[] = $name;
    }
    
    
    /**
     * Delete a queue and all of it's messages
     * 
     * @param  string  $name queue name
     * @return boolean
     * @throws Zend_Queue_Exception
     * @see Zend_Queue_Adapter_AdapterInterface::delete()
     */
    public function delete($name) {
        $key = $this->_getKey($name);
        if ( $this->_cache->del($key) ) {
            $index = array_search($name, $this->_queues);
            if ($key !== false) {
                unset($this->_queues[$index]);
            }
        }
        return false;
    }
    
    /**
     * Get an array of all available queues
     * @see Zend_Queue_Adapter_AdapterAbstract::getQueue()
     * @return array
     */
    public function getQueues()
    {
        $this->_queues = array();
        $queues = $this->_cache->keys($this->_keyPrefix.'_*');
        if ( is_array($queues ) ) {
            foreach( $queues as $name ) {
                $this->_queues[] = str_replace($this->_keyPrefix, '', $name);
            }
        }
        return $this->_queues;
    }
    
    /**
     * Return the approximate number of messages in the queue
     * @see Zend_Queue_Adapter_AdapterInterface::count()
     * @param  Zend_Queue $queue
     * @return integer
     * @throws Zend_Queue_Exception (not supported)
     */
    public function count(Zend_Queue $queue=null)
    {
        if ( is_null( $queue ) ) {
            $queue = $this->_queue;
        }
        $name = $queue->getName();
        if ( !$this->isExists($name) ) {
            require_once 'Zend/Queue/Exception.php';
            throw new Zend_Queue_Exception('Queue does not exist:' . $queueName);
        }
        $key = $this->_getKey($name);
        return $this->_cache->lSize($key);
    }
    
    /********************************************************************
     * Messsage management functions
     *********************************************************************/
    
   /**
     * Send a message to the queue
     *
     * @param  string     $message Message to send to the active queue
     * @param  Zend_Queue $queue
     * @return Zend_Queue_Message
     * @throws Zend_Queue_Exception
     */
    public function send($message, Zend_Queue $queue = null ) {
        if ($queue === null) {
            $queue = $this->_queue;
        }

        if (!$this->isExists($queue->getName())) {
            require_once 'Zend/Queue/Exception.php';
            throw new Zend_Queue_Exception('Queue does not exist:' . $queue->getName());
        }
        
        $message = (string) $message;
        $data    = array(
            'message_id' => md5(uniqid(rand(), true)),
            'handle'     => null,
            'body'       => $message,
            'md5'        => md5($message),
        );
        $key = $this->_getKey($queue->getName());
        $result = $this->_cache->lPush($key,$message);
        if ( $result === false ) {
            require_once 'Zend/Queue/Exception.php';
            throw new Zend_Queue_Exception('failed to insert message into queue:' . $queue->getName());
        }
        
        $options = array(
            'queue' => $queue,
            'data'  => $data,
        );

        $classname = $queue->getMessageClass();
        if (!class_exists($classname)) {
            require_once 'Zend/Loader.php';
            Zend_Loader::loadClass($classname);
        }
        return new $classname($options);
    }
    
    /**
     * Get messages in the queue
     *
     * @param  integer    $maxMessages  Maximum number of messages to return
     * @param  integer    $timeout      Visibility timeout for these messages
     * @param  Zend_Queue $queue
     * @return Zend_Queue_Message_Iterator
     * @throws Zend_Queue_Exception
     */    
    public function receive( $maxMessages = null, $timeout=null, Zend_Queue $queue = null ) {
        if ($maxMessages === null) {
            $maxMessages = 1;
        }

        if ($timeout === null) {
            $timeout = self::RECEIVE_TIMEOUT_DEFAULT;
        }
        if ($queue === null) {
            $queue = $this->_queue;
        }
        $msgs = array();
        if ( $maxMessages > 0 ) {
            for( $i = 0; $i < $maxMessages; $i++ ) {
                $message = $this->_cache->lGet($this->_getKey($queue->getName()),$i);
                if ( $message != "" ) { 
                    $msgs[] = array(
                        'body' => $message,
                    );
                }
            }
        }
        
        $options = array(
            'queue'        => $queue,
            'data'         => $msgs,
            'messageClass' => $queue->getMessageClass(),
        );

        $classname = $queue->getMessageSetClass();
        if (!class_exists($classname)) {
            require_once 'Zend/Loader.php';
            Zend_Loader::loadClass($classname);
        }
        return new $classname($options);
    }
    

    /********************************************************************
     * Supporting functions
     *********************************************************************/

    /**
     * Return a list of queue capabilities functions
     *
     * $array['function name'] = true or false
     * true is supported, false is not supported.
     *
     * @param  string $name
     * @return array
     */
    public function getCapabilities()
    {
        return array(
            'create'        => true,
            'delete'        => true,
            'send'          => true,
            'receive'       => true,
            'deleteMessage' => true,
            'getQueues'     => true,
            'count'         => true,
            'isExists'      => true,
        );
    }
    
    /**
     * Delete a message from the queue
     *
     * Returns true if the message is deleted, false if the deletion is
     * unsuccessful.
     *
     * @param  Zend_Queue_Message $message
     * @return boolean
     * @throws Zend_Queue_Exception (unsupported)
     */
    public function deleteMessage(Zend_Queue_Message $message)
    {
        $name = $this->_queue->getName();
        $key = $this->_getKey($name);
        return (boolean)$this->_cache->lRem($key,$message->body,1);
    }
    
    
   /********************************************************************
     * Functions that are not part of the Zend_Queue_Adapter_Abstract
     *********************************************************************/
    
    /**
     * Returns the name of the key with the prefix
     * @param string $name
     * @return string
     */
    protected function _getKey($name) {
        return $this->_keyPrefix.'_'.$name;
    }
}