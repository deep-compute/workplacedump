import facebook

graph = facebook.GraphAPI("DQVJ2bTZATVDZAtN2JwYUszMlRRQWNQazRUM0JxUkYtLVdoQnVEa2k1a08wczQ1WTdpUWduVzRZAUHVGbVdyN3h3LTItUi1zcmFoekhkNXV1TEJRc3ZAsSjIteUlZAbjg3ZATZAWYVJsQWJ6VlRaYVhsWXl0WDgxdUI5SjlIc0dEVVJMaTJJSWVyTkJUTjVtR010cXBSd0l2UGY4T0g0TGRmWWZAnNjlsZAGZA0ZA0pHODAwbndsOWlGV3E2cGxneGU1R0N1Y2pnQjUwcC1fQVBhakxkc2haYwZDZD")
post = graph.put_object(parent_object="188525715237426", connection_name="feed", message="Please Block Us Facebook.")
counter = 1
comment = graph.put_object(parent_object=post['id'], connection_name="comments", message="@[100023844226537] How dare you block us..")
counter= counter + 1
print counter
while True:
    graph.put_object(parent_object=comment['id'], connection_name="comments", message="How dare you block us..")
    counter = counter+1
    print counter
