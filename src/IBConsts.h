#pragma once
/// constants for initializing queues
#define ib_send_completion_queue_depth  8192
#define ib_recv_completion_queue_depth  8192
#define ib_send_queue_depth  8192          
#define ib_receive_queue_depth  8192       
#define ib_scatter_gather_element_count  2 
#define ib_max_inline_data  16             
#define ib_max_dest_rd_atomic  16          
#define ib_max_rd_atomic  16               
#define ib_min_rnr_timer  0x12             
#define ib_timeout  0x12                   
#define ib_retry_count  6                  
#define ib_rnr_retry  0                    