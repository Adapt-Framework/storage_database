<?php

namespace adapt\storage_database{
    
    /* Prevent Direct Access */
    defined('ADAPT_STARTED') or die;
    
    class storage_database extends \adapt\base{
        
        protected $_private_data_source;
        
        public function __construct($data_source = null){
            parent::__construct();
            $this->_private_data_source = $data_source;
        }
        
        public function pget_data_source(){
            if ($this->_private_data_source && $this->_private_data_source instanceof data_source_sql){
                return $this->_private_data_source;
            }else{
                return parent::pget_data_source();
            }
        }
        
        public function pget_available(){ //For compatability with \adapt\storage_file_system
            return true;
        }
        
        public function is_key_valid($key){
            if (!strpos($key, "..")){
                return preg_match("/[0-9a-zA-Z]+(\/?[.-_A-Za-z0-9]+)*/", $key);
            }
            
            return false;
        }
        
        public function get_new_key(){
            return guid();
        }
        
        public function set($key, $data, $content_type = null, $public = false){
            $model_file = new model_file();
            
            $model_file->load_by_key($key);
            $model_file->errors(true);
            $model_file->file_key = $key;
            $model_file->mime_type = $content_type;
            $model_file->save();
            $model_file->set_data($data);
        }
        
        public function set_by_file($key, $path, $content_type = null, $public = false){
            if (file_exists($path)){
                
                $model_file = new model_file();
                
                $model_file->load_by_key($key);
                $model_file->errors(true);
                $model_file->file_key = $key;
                $model_file->mime_type = $content_type;
                $model_file->save();
                $model_file->set_data_by_file($path);
            }
        }
        
        public function get($key, $number_of_bytes = null, $offset = 0){
            $model_file = new model_file();
            
            if ($model_file->load_by_key($key)){
                return $model_file->get_data($offset, $number_of_bytes);
            }
            
            $this->error("Invalid file storage key '{$key}'");
            return null;
        }
        
        public function write_to_file($key, $path = null){
            $model = new model_file($key, $path);
            
            if ($model->load_by_key($key)){
                return $model->write_to_file($path);
            }
            
            $this->error("Invalid file key '{$key}'");
            return false;
        }
        
        public function delete($key){
            $model = new model_file();
            
            if ($model->load_by_key($key)){
                $model->delete();
                return true;
            }else{
                $this->error("Invalid file key '{$key}'");
            }
            
            return false;
        }
        
        public function get_size($key){
            $model = new model_file();
            
            if ($model->load_by_key($key)){
                return $model->file_size;
            }
            
            return 0;
        }
        
        public function set_content_type($key, $content_type = null){
            $model = new model_file();
            if ($model->load_by_key($key)){
                $model->mime_type = $content_type;
                $model->save();
            }
        }
        
        public function get_content_type($key){
            $model = new model_file();
            if ($model->load_by_key($key)){
                return $model->mime_type;
            }
            
            return null;
        }
        
        public function set_meta_data($key, $tag, $value){
            $model = new model_file();
            
            if ($model->load_by_key($key)){
                $model->meta_data($tag, $value);
                $model->save();
            }
        }
        
        public function get_meta_data($key, $tag){
            $model = new model_file();
            
            if ($model->load_by_key($key)){
                return $model->meta_data($tag);
            }
            
            return null;
        }
    }
    
}

?>