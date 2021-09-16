SELECT 
    time, 
    username,
    '{course_id}' as course_id,
    case 
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/threads/[^/]+/reply') then "reply"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/threads/[^/]+/upvote') then "upvote"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/threads/[^/]+/unvote') then "unvote"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/threads/[^/]+/update') then "update"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/threads/[^/]+/delete') then "delete"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/threads/[^/]+/close') then "close"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/threads/[^/]+/follow') then "follow_thread"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/threads/[^/]+/unfollow') then "unfollow_thread"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/threads/[^/]+/pin') then "pin"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/threads/[^/]+/unpin') then "unpin"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/threads/[^/]+/downvote') then "downvote"  
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/comments/[^/]+/reply') then "comment_reply"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/comments/[^/]+/upvote') then "comment_upvote"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/comments/[^/]+/update') then "comment_update"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/comments/[^/]+/unvote') then "comment_unvote"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/comments/[^/]+/delete') then "comment_delete"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/forum/users/[^/]+/followed') then "follow_user"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/forum/users/[^/]+$') then "target_user"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/forum/[^/]+/threads/[^/]+') then "read"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/forum/[^/]+/inline') then "read_inline"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/forum/search') then "search"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/forum$') then "enter_forum"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/forum/$') then "enter_forum"
        when REGEXP_CONTAINS(event_type, r'/courses/(.*)/instructor/api/(.*)') then REGEXP_EXTRACT(event_type, r'/courses/.*/instructor/api/(.*)')
        when event_type = "edx.forum.thread.created" then "created_thread"
        when event_type = "edx.forum.response.created" then "created_response"
        when event_type = "edx.forum.comment.created" then "created_comment"
        when event_type = "edx.forum.searched" then "searched"
        else event_type 
    end as forum_action,
    case 
        when module_id is not null then REGEXP_EXTRACT(module_id, r'[^/]+/[^/]+/forum/([^/]+)') 
        else (
            case 
                when module_id is null 
                    then (case 
                            when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/forum/[^/]+/threads/[^/]+') 
                                then REGEXP_EXTRACT(event_type, r'/courses/.*/discussion/forum/[^/]+/threads/([^/]+)') 
                            else 
                                (case 
                                    when REGEXP_CONTAINS(event_type, r'/courses/(.*)/discussion/threads/[^/]+/') 
                                        then REGEXP_EXTRACT(event_type, r'/courses/.*/discussion/threads/([^/]+)') 
                                    else REGEXP_EXTRACT(event_type, r'/courses/.*/discussion/comments/([^/]+)/') 
                                end) 
                        end) 
            end) 
    end as thread_id,
    REGEXP_EXTRACT(event_type, r'/courses/.*/forum/([^/]+)/') as subject,
    REGEXP_EXTRACT(event_type, r'/courses/.*/forum/users/([^/]+)') as target_user_id,
    event_struct.query as search_query,   
    event_struct.GET as event_GET,        
FROM `{{ log_dataset }}.tracklog_*`
WHERE {% if suffix_start is defined and suffix_end is defined %} (_TABLE_SUFFIX BETWEEN "{{ suffix_start }}" AND "{{ suffix_end }}") AND {% endif %}
(
    REGEXP_CONTAINS(event_type ,r'^edx\.forum\..*')
    OR event_type like "%/discussion/forum%"
    OR event_type like "%/discussion/threads%"
    OR event_type like "%/discussion/comments%"
    OR event_type like "%list-forum-%"
    OR event_type like "%list_forum_%"
    OR event_type like "%add-forum-%"
    OR event_type like "%add_forum_%"
    OR event_type like "%remove-forum-%"
    OR event_type like "%remove_forum_%"
    OR event_type like "%update_forum_%"
) 
AND username is not null
AND event is not null
ORDER BY time