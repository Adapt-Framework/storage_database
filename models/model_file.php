<?php

namespace adapt\storage_database{
    
    /* Prevent Direct Access */
    defined('ADAPT_STARTED') or die;
    
    class model_file extends \adapt\model{
        
        const EVENT_ON_LOAD_BY_KEY = 'model_file.on_load_by_key';
        
        public function __construct($id = null, $data_source = null){
            parent::__construct('file', $id, $data_source);
        }
        
        /* Over-ride the initialiser to auto load children */
        public function initialise(){
            /* We must initialise first! */
            parent::initialise();
            
            /* We need to limit what we auto load */
            $this->_auto_load_only_tables = array(
                'file_meta_data'
            );
            
            /* Switch on auto loading */
            $this->_auto_load_children = true;
        }
        
        public function load_by_key($key){
            $this->initialise();
            
            /* Make sure name is set */
            if (isset($key)){
                
                $sql = $this->data_source->sql;
                
                $sql->select('*')
                    ->from($this->table_name)
                    ->where(
                        new sql_and(
                            new sql_cond('file_key', sql::EQUALS, sql::q($key)),
                            new sql_cond('date_deleted', sql::IS, new sql_null())
                        )
                    );
                
                /* Get the results */
                $results = $sql->execute()->results();
                
                if (count($results) == 1){
                    $this->trigger(self::EVENT_ON_LOAD_BY_KEY);
                    return $this->load_by_data($results[0]);
                }elseif(count($results) == 0){
                    $this->error("Unable to find a record with the file key '{$key}'");
                }elseif(count($results) > 1){
                    $this->error(count($results) . " records found for file key '{$key}'.");
                }
                
                
            }else{
                $this->error('Unable to load by file key, no file key supplied');
            }
            
            return false;
        }
        
        public function delete(){
            if ($this->setting('adapt.storage_database.actual_delete') == "Yes"){
                if ($this->is_loaded){
                    /* Delete any blocks we already have */
                    $sql = $this->data_source->sql;
                    
                    $sql->delete_from('file_block')
                        ->where(
                            new sql_cond('file_id', sql::EQUALS, sql::q($this->file_id))
                        );
                    
                    $sql->execute();
                    $this->initialise();
                }
            }else{
                if ($this->is_loaded){
                    /* Delete any blocks we already have */
                    $sql = $this->data_source->sql;
                    
                    $sql->update('file_block')
                        ->set('date_deleted', new sql_now())
                        ->where(
                            new sql_and(
                                new sql_cond('file_id', sql::EQUALS, sql::q($this->file_id)),
                                new sql_cond('date_deleted', sql::IS, new sql_null())
                            )
                        );
                        
                    $sql->execute();
                    $this->initialise();
                }
                parent::delete();
            }
        }
        
        public function set_data_by_file($path){
            if ($this->is_loaded){
                
                if (file_exists($path)){
                    
                    /* Delete any blocks we already have */
                    $sql = $this->data_source->sql;
                    
                    if ($this->setting('adapt.storage_database.actual_delete') == "Yes"){
                        $sql->delete_from('file_block')
                            ->where(
                                new sql_cond('file_id', sql::EQUALS, sql::q($this->file_id))
                            );
                    }else{
                        $sql->update('file_block')
                            ->set('date_deleted', new sql_now())
                            ->where(
                                new sql_and(
                                    new sql_cond('file_id', sql::EQUALS, sql::q($this->file_id)),
                                    new sql_cond('date_deleted', sql::IS, new sql_null())
                                )
                            );
                    }
                    
                    $sql->execute();
                    
                    /* Set the md5 */
                    $this->md5 = md5_file($path);
                    
                    /* Set the file size */
                    $this->file_size = filesize($path);
                    
                    /* Set the block size */
                    $this->block_size = intval($this->setting("adapt.storage_database.block_size")) * 1024;
                    
                    if ($this->file_size < $this->block_size){
                        $this->block_size = $this->file_size;
                    }
                    
                    /* Set the file */
                    
                    /* Open the file */
                    $fp = fopen($path, "rb");
                    
                    /* Check we have a valid file handle */
                    if ($fp){
                        /* Loop thru and store each block */
                        $bytes_remaining = $this->file_size;
                        $block_count = 0;
                        while($bytes_remaining > 0){
                            $block = new model_file_block();
                            $block->file_id = $this->file_id;
                            $block->priority = strval(++$block_count);
                            
                            if ($bytes_remaining > $this->block_size){
                                $block->block_size = $this->block_size;
                                $block->data = base64_encode(fread($fp, $this->block_size));
                                $bytes_remaining -= $this->block_size;
                            }else{
                                $block->block_size = $bytes_remaining;
                                $block->data = base64_encode(fread($fp, $bytes_remaining));
                                $bytes_remaining = 0;
                            }
                            
                            /* Save the block to the database */
                            $block->save();
                        }
                        
                        /* Close the file */
                        fclose($fp);
                    }
                    
                    return $this->save();
                }else{
                    $this->error("File '{$path}' does not exist");
                    return false;
                }
                
            }else{
                $this->error("You must save before setting data");
                return false;
            }
        }
        
        public function set_data($data){
            if ($this->is_loaded){
                
                if ($data){
                    
                    /* Delete any blocks we already have */
                    $sql = $this->data_source->sql;
                    
                    if ($this->setting('adapt.storage_database.actual_delete') == "Yes"){
                        $sql->delete_from('file_block')
                            ->where(
                                new sql_cond('file_id', sql::EQUALS, sql::q($this->file_id))
                            );
                    }else{
                        $sql->update('file_block')
                            ->set('date_deleted', new sql_now())
                            ->where(
                                new sql_and(
                                    new sql_cond('file_id', sql::EQUALS, sql::q($this->file_id)),
                                    new sql_cond('date_deleted', sql::IS, new sql_null())
                                )
                            );
                    }
                    
                    $sql->execute();
                    
                    /* Set the md5 */
                    $this->md5 = md5($data);
                    
                    /* Set the file size */
                    $this->file_size = strlen($data);
                    $this->block_size = $this->file_size;
                    
                    /* Set the data */
                    $block = new model_file_block();
                    $block->file_id = $this->file_id;
                    $block->block_size = $this->file_size;
                    $block->data = base64_encode($data);
                    $block->priority = '0';
                    $block->save();
                    
                    
                    return $this->save();
                }else{
                    $this->error("No data provided");
                    return false;
                }
                
            }else{
                $this->error("You must save before setting data");
                return false;
            }
        }
        
        public function get_data($offset = 0, $bytes = null){
            $data = "";
            if ($this->is_loaded){
                if ($offset < $this->file_size){
                    if (!isset($bytes) || $bytes + $offset > $this->file_size){
                        $bytes = $this->file_size - $offset;
                    }
                    $block_count = $this->get_block_count();
                    $blocks_to_fetch = array();
                    $blocks_to_fetch_count = 0;
                    
                    while($bytes > 0){
                        if ($offset == 0){
                            $blocks_to_fetch[$blocks_to_fetch_count] = 0;
                        }else{
                            $blocks_to_fetch[$blocks_to_fetch_count] = floor($offset / $this->block_size);
                        }
                        
                        $block_offset = $offset - ($blocks_to_fetch[$blocks_to_fetch_count] * $this->block_size);
                        
                        $bytes_from_block = $this->block_size - $block_offset;
                        if ($bytes_from_block > $bytes){
                            $bytes_from_block = $bytes;
                            $bytes = 0;
                        }else{
                            $bytes -= $bytes_from_block;
                            $offset += $bytes_from_block;
                        }
                        
                        /* Retrieve the block */
                        $block = $this->get_block_data($blocks_to_fetch[$blocks_to_fetch_count]+1);
                        
                        $data .= substr($block, $block_offset, $bytes_from_block);
                        
                        $blocks_to_fetch_count++;
                    }
                }
            }
            
            return $data;
        }
        
        public function get_block_count(){
            if ($this->is_loaded){
                $sql = $this->data_source->sql;
                
                $sql->select('file_block_id')
                    ->from('file_block')
                    ->where(
                        new sql_and(
                            new sql_cond('file_id', sql::EQUALS, sql::q($this->file_id)),
                            new sql_cond('date_deleted', sql::IS, new sql_null())
                        )
                    );
                    
                $results = $sql->execute()->results();
                
                return count($results);
            }
            
            return 0;
        }
        
        public function get_block_data($index){
            if ($this->is_loaded){
                $data = $this->cache->get("storage_database/file-{$this->file_id}/block-{$index}");
                
                if (!$data){
                    $sql = $this->data_source->sql;
                    
                    $sql->select('data')
                        ->from('file_block')
                        ->where(
                            new sql_and(
                                new sql_cond('file_id', sql::EQUALS, sql::q($this->file_id)),
                                new sql_cond('priority', sql::EQUALS, sql::q($index)),
                                new sql_cond('date_deleted', sql::IS, new sql_null())
                            )
                        );
                    
                    $results = $sql->execute()->results();
                    
                    if (count($results) == 1){
                        $data = base64_decode($results[0]['data']);
                        $this->cache->set("storage_database/file-{$this->file_id}/block-{$index}", $data, 60 * 60 * 24 * 30);
                        
                        return $data;
                    }else{
                        $this->error("Unable to load block.");
                    }
                }
                
                return null;
            }
            
        }
        
        public function write_to_file($path){
            if ($this->is_loaded){
                
                /* We are going to write a block at a time
                 * so that large files don't cause issues
                 */
                if ($fp = fopen($path, "wb")){
                    
                    $block_count = $this->get_block_count();
                    
                    for($i = 0; $i < $block_count; $i++){
                        fwrite($fp, $this->get_block_data($i));
                    }
                    
                    fclose($fp);
                    
                    return true;
                    
                }else{
                    $this->error("Unable to write to '{$path}'");
                }
                
            }else{
                $this->error("File not loaded");
            }
            
            
            return false;
        }
        
        public function meta_data($key, $value = null){
            $children = $this->get();
            
            if (is_null($value)){
                foreach($children as $child){
                    if ($child instanceof \adapt\model && $child->table_name == 'file_meta_data'){
                        if ($child->name == $key){
                            return $child->value;
                        }
                    }
                }
                
            }else{
                foreach($children as $child){
                    if ($child instanceof \adapt\model && $child->table_name == 'file_meta_data'){
                        if ($child->name == $key){
                            $child->value = $value;
                            return null;
                        }
                    }
                }
                
                /* We didn't find the setting, so let create a new one */
                $setting = new model_file_meta_data();
                $setting->name = $key;
                $setting->value = $value;
                $this->add($setting);
            }
            
            return null;
        }

    }
    
}


?>