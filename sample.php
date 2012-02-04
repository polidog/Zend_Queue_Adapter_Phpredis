<?php
require_once 'Zend/Loader/Autoloader.php';
$autoloader = Zend_Loader_Autoloader::getInstance();

try {
	$queue = new Zend_Queue('Phpredis',array(
		'name' => 'test_queue',
		'driverOptions' => array(
		),
	));
	
	
	// send message
	$queue->send('job 1');
	$queue->send('job 2');
	echo "send end\n\n";
	
	// get message
	$messages = $queue->receive(3);
	foreach ( $messages as $message ) {
		echo $message->body . "\n";
	}	
	/*
	 * job 1
	 * job 2
	 */
	
	
} catch ( Zend_Queue_Exception $e ) {
	echo $e->getMessage();
}