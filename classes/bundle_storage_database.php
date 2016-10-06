<?php

namespace adapt\storage_database{
    
    /* Prevent Direct Access */
    defined('ADAPT_STARTED') or die;
    
    class bundle_storage_database extends \adapt\bundle{
        
        public function __construct($data){
            parent::__construct('storage_database', $data);
        }
        
        public function boot(){
            if (parent::boot()){
                
                if ($this->setting('adapt.storage_database.is_default') == "Yes"){
                    /* We are going to replace the default file store with our own */
                    $this->file_store = new storage_database();
                }
                
                return true;
            }
            
            return false;
        }
        
        
    }
}

?>