SET echo;

CREATE TABLE users ( \
  ID int, \
  PII_Name text, \
  PRIMARY KEY(ID) \
);

CREATE TABLE products ( \
  ID int, \
  Name text, \
  price int, \
  PRIMARY KEY(ID) \
);

CREATE TABLE purchases ( \
  user_id int, \
  product_id int, \
  timestamp int, \
  price int, \
  FOREIGN KEY (user_id) REFERENCES users(ID), \
  FOREIGN KEY (product_id) REFERENCES products(ID) \
);

CREATE TABLE ratings ( \
  user_id int, \
  rating int, \
  product_id int, \
  FOREIGN KEY (user_id) REFERENCES users(ID), \
  FOREIGN KEY (product_id) REFERENCES products(ID) \
);

CREATE VIEW v1_ratings AS \
  '"SELECT user_id, product_id rating FROM ratings"';

CREATE VIEW v2_avg_rating_per_product AS \
  '"SELECT products.ID,products.Name, SUM(ratings.rating) AS AVG_RATING \
   FROM products JOIN ratings ON products.ID = ratings.product_id GROUP BY products.ID, products.Name"';

CREATE VIEW v3_avg_sales_per_product AS \
  '"SELECT products.ID,products.Name, SUM(purchases.price) AS AVG_SALES \
   FROM products JOIN purchases ON products.ID = purchases.product_id GROUP BY products.ID, products.Name"';


CREATE VIEW v4_sales AS \
  '"SELECT price, product_id, user_id FROM purchases"';

CREATE VIEW v5_avg_sales_per_user AS \
  '"SELECT users.ID,users.PII_Name, SUM(purchases.price) \
   FROM users JOIN purchases ON users.ID = purchases.user_id GROUP BY users.ID, users.PII_Name"';

INSERT INTO users VALUES (0, 'user_0', 'ratings,sales');
INSERT INTO users VALUES (1, 'user_1', '');
INSERT INTO users VALUES (2, 'user_2', 'avg_rating_per_product');
INSERT INTO users VALUES (3, 'user_3', 'ratings,avg_rating_per_product,sales');
INSERT INTO users VALUES (4, 'user_4', 'ratings,sales');
INSERT INTO users VALUES (5, 'user_5', 'ratings,avg_sales_per_product,sales');
INSERT INTO users VALUES (6, 'user_6', 'ratings,avg_sales_per_product');
INSERT INTO users VALUES (7, 'user_7', 'ratings,sales');
INSERT INTO users VALUES (8, 'user_8', 'ratings');
INSERT INTO users VALUES (9, 'user_9', 'ratings,avg_rating_per_product,sales');
INSERT INTO products VALUES (0, 'product_0',87,'ratings,sales');
INSERT INTO products VALUES (1, 'product_1',82,'');
INSERT INTO products VALUES (2, 'product_2',81,'avg_rating_per_product');
INSERT INTO products VALUES (3, 'product_3',82,'ratings,avg_rating_per_product,sales');
INSERT INTO products VALUES (4, 'product_4',39,'ratings,sales');
INSERT INTO products VALUES (5, 'product_5',82,'ratings,avg_sales_per_product,sales');
INSERT INTO products VALUES (6, 'product_6',58,'ratings,avg_sales_per_product');
INSERT INTO products VALUES (7, 'product_7',14,'ratings,sales');
INSERT INTO products VALUES (8, 'product_8',67,'ratings');
INSERT INTO products VALUES (9, 'product_9',31,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (5, 4, 257, 76 ,'ratings,avg_sales_per_product,sales');
INSERT INTO purchases VALUES (0, 0, 430, 89 ,'ratings,sales');
INSERT INTO purchases VALUES (4, 2, 165, 87 ,'ratings,sales');
INSERT INTO purchases VALUES (5, 5, 249, 49 ,'ratings,avg_sales_per_product,sales');
INSERT INTO purchases VALUES (1, 3, 130, 66 ,'');
INSERT INTO purchases VALUES (6, 0, 276, 88 ,'ratings,avg_sales_per_product');
INSERT INTO purchases VALUES (9, 8, 190, 90 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (9, 2, 974, 29 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (9, 4, 351, 99 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (1, 6, 339, 67 ,'');
INSERT INTO purchases VALUES (8, 5, 809, 39 ,'ratings');
INSERT INTO purchases VALUES (6, 2, 429, 70 ,'ratings,avg_sales_per_product');
INSERT INTO purchases VALUES (0, 3, 626, 43 ,'ratings,sales');
INSERT INTO purchases VALUES (8, 2, 573, 38 ,'ratings');
INSERT INTO purchases VALUES (5, 1, 573, 11 ,'ratings,avg_sales_per_product,sales');
INSERT INTO purchases VALUES (0, 7, 496, 61 ,'ratings,sales');
INSERT INTO purchases VALUES (4, 0, 335, 85 ,'ratings,sales');
INSERT INTO purchases VALUES (3, 7, 846, 38 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (9, 1, 391, 13 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (5, 2, 717, 48 ,'ratings,avg_sales_per_product,sales');
INSERT INTO purchases VALUES (7, 7, 658, 29 ,'ratings,sales');
INSERT INTO purchases VALUES (2, 6, 536, 70 ,'avg_rating_per_product');
INSERT INTO purchases VALUES (4, 2, 624, 94 ,'ratings,sales');
INSERT INTO purchases VALUES (5, 8, 979, 27 ,'ratings,avg_sales_per_product,sales');
INSERT INTO purchases VALUES (7, 3, 953, 34 ,'ratings,sales');
INSERT INTO purchases VALUES (3, 8, 698, 79 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (8, 7, 901, 38 ,'ratings');
INSERT INTO purchases VALUES (7, 0, 312, 67 ,'ratings,sales');
INSERT INTO purchases VALUES (3, 6, 704, 34 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (1, 9, 510, 66 ,'');
INSERT INTO purchases VALUES (4, 0, 837, 67 ,'ratings,sales');
INSERT INTO purchases VALUES (3, 5, 186, 95 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (2, 1, 785, 67 ,'avg_rating_per_product');
INSERT INTO purchases VALUES (0, 5, 521, 67 ,'ratings,sales');
INSERT INTO purchases VALUES (1, 4, 497, 62 ,'');
INSERT INTO purchases VALUES (7, 1, 199, 79 ,'ratings,sales');
INSERT INTO purchases VALUES (3, 6, 846, 98 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (7, 7, 541, 14 ,'ratings,sales');
-- INSERT INTO purchases VALUES (6, 5, 611, 42 ,'ratings,avg_sales_per_product');
INSERT INTO purchases VALUES (0, 4, 434, 14 ,'ratings,sales');
INSERT INTO purchases VALUES (0, 1, 513, 71 ,'ratings,sales');
INSERT INTO purchases VALUES (8, 7, 110, 45 ,'ratings');
INSERT INTO purchases VALUES (9, 9, 726, 37 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (5, 2, 489, 54 ,'ratings,avg_sales_per_product,sales');
INSERT INTO purchases VALUES (2, 2, 244, 30 ,'avg_rating_per_product');
INSERT INTO purchases VALUES (0, 0, 898, 14 ,'ratings,sales');
INSERT INTO purchases VALUES (1, 6, 532, 56 ,'');
INSERT INTO purchases VALUES (2, 6, 701, 83 ,'avg_rating_per_product');
INSERT INTO purchases VALUES (8, 3, 983, 28 ,'ratings');
INSERT INTO purchases VALUES (2, 0, 683, 23 ,'avg_rating_per_product');
INSERT INTO purchases VALUES (1, 5, 889, 13 ,'');
INSERT INTO purchases VALUES (0, 7, 491, 95 ,'ratings,sales');
INSERT INTO purchases VALUES (1, 3, 819, 48 ,'');
INSERT INTO purchases VALUES (6, 0, 843, 94 ,'ratings,avg_sales_per_product');
INSERT INTO purchases VALUES (2, 1, 468, 88 ,'avg_rating_per_product');
INSERT INTO purchases VALUES (8, 5, 597, 70 ,'ratings');
INSERT INTO purchases VALUES (2, 7, 531, 74 ,'avg_rating_per_product');
INSERT INTO purchases VALUES (0, 6, 492, 27 ,'ratings,sales');
INSERT INTO purchases VALUES (7, 8, 892, 52 ,'ratings,sales');
INSERT INTO purchases VALUES (3, 3, 191, 66 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (7, 2, 730, 54 ,'ratings,sales');
INSERT INTO purchases VALUES (9, 8, 466, 82 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (2, 8, 428, 15 ,'avg_rating_per_product');
INSERT INTO purchases VALUES (6, 0, 128, 37 ,'ratings,avg_sales_per_product');
INSERT INTO purchases VALUES (1, 3, 679, 65 ,'');
INSERT INTO purchases VALUES (1, 8, 772, 98 ,'');
INSERT INTO purchases VALUES (3, 5, 107, 36 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (0, 7, 349, 56 ,'ratings,sales');
INSERT INTO purchases VALUES (5, 2, 289, 93 ,'ratings,avg_sales_per_product,sales');
INSERT INTO purchases VALUES (4, 2, 456, 99 ,'ratings,sales');
INSERT INTO purchases VALUES (7, 7, 437, 55 ,'ratings,sales');
INSERT INTO purchases VALUES (6, 2, 365, 96 ,'ratings,avg_sales_per_product');
INSERT INTO purchases VALUES (7, 9, 140, 90 ,'ratings,sales');
INSERT INTO purchases VALUES (1, 6, 956, 10 ,'');
INSERT INTO purchases VALUES (0, 8, 239, 70 ,'ratings,sales');
INSERT INTO purchases VALUES (5, 8, 821, 71 ,'ratings,avg_sales_per_product,sales');
INSERT INTO purchases VALUES (5, 6, 136, 51 ,'ratings,avg_sales_per_product,sales');
INSERT INTO purchases VALUES (9, 4, 790, 28 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (4, 2, 681, 16 ,'ratings,sales');
INSERT INTO purchases VALUES (8, 7, 840, 63 ,'ratings');
INSERT INTO purchases VALUES (7, 0, 498, 86 ,'ratings,sales');
INSERT INTO purchases VALUES (0, 5, 367, 78 ,'ratings,sales');
INSERT INTO purchases VALUES (2, 8, 177, 82 ,'avg_rating_per_product');
INSERT INTO purchases VALUES (4, 0, 792, 91 ,'ratings,sales');
INSERT INTO purchases VALUES (8, 7, 748, 91 ,'ratings');
INSERT INTO purchases VALUES (6, 8, 312, 90 ,'ratings,avg_sales_per_product');
INSERT INTO purchases VALUES (1, 5, 812, 39 ,'');
INSERT INTO purchases VALUES (4, 2, 153, 99 ,'ratings,sales');
-- INSERT INTO purchases VALUES (6, 5, 980, 85 ,'ratings,avg_sales_per_product');
INSERT INTO purchases VALUES (5, 2, 859, 84 ,'ratings,avg_sales_per_product,sales');
INSERT INTO purchases VALUES (0, 1, 171, 97 ,'ratings,sales');
INSERT INTO purchases VALUES (9, 7, 297, 95 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (2, 0, 141, 91 ,'avg_rating_per_product');
INSERT INTO purchases VALUES (3, 0, 473, 27 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (0, 5, 532, 91 ,'ratings,sales');
INSERT INTO purchases VALUES (2, 4, 402, 75 ,'avg_rating_per_product');
INSERT INTO purchases VALUES (7, 9, 558, 25 ,'ratings,sales');
INSERT INTO purchases VALUES (0, 1, 268, 90 ,'ratings,sales');
INSERT INTO purchases VALUES (6, 7, 594, 57 ,'ratings,avg_sales_per_product');
INSERT INTO purchases VALUES (2, 6, 677, 91 ,'avg_rating_per_product');
INSERT INTO purchases VALUES (4, 9, 959, 51 ,'ratings,sales');
INSERT INTO purchases VALUES (4, 5, 642, 33 ,'ratings,sales');
INSERT INTO purchases VALUES (2, 4, 256, 31 ,'avg_rating_per_product');
INSERT INTO purchases VALUES (3, 2, 728, 53 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (0, 5, 863, 71 ,'ratings,sales');
INSERT INTO purchases VALUES (0, 4, 565, 72 ,'ratings,sales');
INSERT INTO purchases VALUES (4, 1, 695, 34 ,'ratings,sales');
INSERT INTO purchases VALUES (0, 2, 148, 27 ,'ratings,sales');
INSERT INTO purchases VALUES (2, 7, 386, 62 ,'avg_rating_per_product');
INSERT INTO purchases VALUES (1, 0, 533, 26 ,'');
INSERT INTO purchases VALUES (2, 8, 263, 15 ,'avg_rating_per_product');
INSERT INTO purchases VALUES (7, 0, 462, 76 ,'ratings,sales');
-- INSERT INTO purchases VALUES (6, 5, 268, 77 ,'ratings,avg_sales_per_product');
INSERT INTO purchases VALUES (3, 0, 631, 23 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (6, 9, 191, 19 ,'ratings,avg_sales_per_product');
INSERT INTO purchases VALUES (9, 6, 996, 84 ,'ratings,avg_rating_per_product,sales');
INSERT INTO purchases VALUES (5, 0, 526, 96 ,'ratings,avg_sales_per_product,sales');
INSERT INTO purchases VALUES (6, 2, 466, 59 ,'ratings,avg_sales_per_product');
INSERT INTO purchases VALUES (7, 9, 651, 11 ,'ratings,sales');
INSERT INTO purchases VALUES (0, 1, 964, 43 ,'ratings,sales');
INSERT INTO ratings VALUES (3, 2, 8, 'ratings,avg_rating_per_product,avg_sales_per_product');
INSERT INTO ratings VALUES (5, 3, 2, 'ratings,avg_rating_per_product,avg_sales_per_product');
INSERT INTO ratings VALUES (5, 4, 9, 'avg_sales_per_product');
INSERT INTO ratings VALUES (9, 3, 0, 'avg_rating_per_product,sales');
INSERT INTO ratings VALUES (8, 3, 7, 'avg_sales_per_product');
INSERT INTO ratings VALUES (7, 3, 4, '');
INSERT INTO ratings VALUES (8, 3, 4, 'avg_rating_per_product,sales');
INSERT INTO ratings VALUES (4, 1, 9, '');
INSERT INTO ratings VALUES (0, 0, 3, 'sales');
INSERT INTO ratings VALUES (5, 0, 0, 'ratings,sales');
INSERT INTO ratings VALUES (1, 0, 7, '');
INSERT INTO ratings VALUES (6, 2, 6, '');
INSERT INTO ratings VALUES (0, 3, 4, 'avg_sales_per_product,sales');
INSERT INTO ratings VALUES (9, 0, 1, 'ratings');
INSERT INTO ratings VALUES (3, 4, 1, 'ratings');
-- INSERT INTO ratings VALUES (9, 2, 2, 'ratings,avg_rating_per_product,avg_sales_per_product');
INSERT INTO ratings VALUES (8, 0, 4, 'avg_sales_per_product');
INSERT INTO ratings VALUES (5, 2, 2, '');
INSERT INTO ratings VALUES (1, 4, 0, '');
INSERT INTO ratings VALUES (5, 0, 9, 'avg_rating_per_product,avg_sales_per_product,sales');
INSERT INTO ratings VALUES (3, 4, 8, 'ratings,avg_sales_per_product,sales');
INSERT INTO ratings VALUES (4, 3, 3, 'avg_rating_per_product');
INSERT INTO ratings VALUES (8, 0, 2, '');
INSERT INTO ratings VALUES (6, 1, 5, 'sales');
INSERT INTO ratings VALUES (4, 1, 6, 'avg_rating_per_product,sales');
INSERT INTO ratings VALUES (6, 4, 5, 'avg_rating_per_product');
INSERT INTO ratings VALUES (7, 0, 1, 'avg_rating_per_product,sales');
INSERT INTO ratings VALUES (9, 4, 5, 'ratings,avg_sales_per_product,sales');
-- INSERT INTO ratings VALUES (4, 3, 9, 'ratings,avg_rating_per_product,avg_sales_per_product');
-- INSERT INTO ratings VALUES (1, 4, 2, 'avg_rating_per_product,sales');
INSERT INTO ratings VALUES (7, 3, 2, 'ratings,avg_sales_per_product');
INSERT INTO ratings VALUES (0, 3, 0, 'ratings,avg_sales_per_product');
INSERT INTO ratings VALUES (7, 2, 1, 'ratings,sales');
-- INSERT INTO ratings VALUES (8, 3, 3, 'ratings,avg_rating_per_product,sales');
INSERT INTO ratings VALUES (6, 0, 9, '');
INSERT INTO ratings VALUES (7, 0, 1, 'avg_sales_per_product');
INSERT INTO ratings VALUES (7, 3, 9, 'avg_sales_per_product');
INSERT INTO ratings VALUES (2, 2, 6, 'ratings,avg_rating_per_product,sales');
INSERT INTO ratings VALUES (1, 2, 9, 'ratings,avg_sales_per_product,sales');
INSERT INTO ratings VALUES (5, 1, 4, 'avg_sales_per_product,sales');
INSERT INTO ratings VALUES (4, 0, 8, 'sales');
-- INSERT INTO ratings VALUES (5, 2, 9, 'avg_rating_per_product');
-- INSERT INTO ratings VALUES (9, 1, 3, 'avg_rating_per_product');
INSERT INTO ratings VALUES (6, 2, 4, 'ratings,avg_sales_per_product');
INSERT INTO ratings VALUES (1, 4, 0, 'ratings,avg_rating_per_product,avg_sales_per_product');
INSERT INTO ratings VALUES (3, 0, 3, '');
INSERT INTO ratings VALUES (0, 2, 9, 'avg_sales_per_product,sales');
INSERT INTO ratings VALUES (1, 2, 4, 'ratings,avg_sales_per_product,sales');
INSERT INTO ratings VALUES (4, 1, 6, 'avg_rating_per_product');
INSERT INTO ratings VALUES (7, 4, 8, 'ratings,sales');
INSERT INTO ratings VALUES (4, 2, 6, 'ratings,avg_rating_per_product');
INSERT INTO ratings VALUES (1, 0, 0, 'avg_sales_per_product,sales');
INSERT INTO ratings VALUES (6, 0, 6, 'ratings');
INSERT INTO ratings VALUES (1, 0, 7, 'ratings');
INSERT INTO ratings VALUES (6, 3, 9, '');
INSERT INTO ratings VALUES (7, 2, 6, 'avg_sales_per_product,sales');
INSERT INTO ratings VALUES (7, 3, 5, '');
INSERT INTO ratings VALUES (2, 0, 5, 'ratings,sales');
INSERT INTO ratings VALUES (2, 3, 9, 'ratings');
INSERT INTO ratings VALUES (9, 2, 8, 'avg_rating_per_product,avg_sales_per_product');
INSERT INTO ratings VALUES (4, 4, 5, 'ratings,avg_rating_per_product');
-- INSERT INTO ratings VALUES (7, 4, 9, 'ratings,avg_rating_per_product,sales');
INSERT INTO ratings VALUES (2, 3, 8, 'avg_sales_per_product,sales');
INSERT INTO ratings VALUES (5, 4, 8, 'ratings,avg_rating_per_product,avg_sales_per_product');
INSERT INTO ratings VALUES (1, 3, 4, 'avg_rating_per_product,sales');
INSERT INTO ratings VALUES (1, 3, 9, 'avg_sales_per_product');
INSERT INTO ratings VALUES (1, 3, 3, '');
INSERT INTO ratings VALUES (4, 1, 8, 'avg_sales_per_product,sales');
INSERT INTO ratings VALUES (5, 4, 8, 'avg_sales_per_product,sales');
INSERT INTO ratings VALUES (7, 2, 5, 'sales');
INSERT INTO ratings VALUES (2, 2, 6, 'ratings,avg_rating_per_product');
INSERT INTO ratings VALUES (6, 3, 0, 'avg_rating_per_product');
INSERT INTO ratings VALUES (1, 2, 8, 'avg_rating_per_product,avg_sales_per_product,sales');
INSERT INTO ratings VALUES (5, 1, 0, '');
INSERT INTO ratings VALUES (9, 3, 4, 'avg_rating_per_product,avg_sales_per_product,sales');
INSERT INTO ratings VALUES (6, 0, 7, 'ratings');
INSERT INTO ratings VALUES (1, 1, 4, '');
INSERT INTO ratings VALUES (9, 4, 4, 'avg_rating_per_product,sales');
INSERT INTO ratings VALUES (4, 4, 4, '');
INSERT INTO ratings VALUES (9, 3, 8, 'ratings,avg_rating_per_product,avg_sales_per_product');
INSERT INTO ratings VALUES (7, 1, 5, 'avg_sales_per_product,sales');
INSERT INTO ratings VALUES (4, 2, 9, '');
INSERT INTO ratings VALUES (1, 0, 8, 'avg_rating_per_product,avg_sales_per_product,sales');
-- INSERT INTO ratings VALUES (7, 0, 2, 'avg_rating_per_product,avg_sales_per_product');
INSERT INTO ratings VALUES (9, 4, 7, 'sales');
INSERT INTO ratings VALUES (6, 0, 6, 'ratings,avg_sales_per_product');
-- INSERT INTO ratings VALUES (1, 3, 9, 'ratings,avg_rating_per_product');
INSERT INTO ratings VALUES (9, 0, 6, 'avg_rating_per_product,sales');
INSERT INTO ratings VALUES (1, 1, 1, 'avg_sales_per_product,sales');
INSERT INTO ratings VALUES (6, 4, 3, 'avg_sales_per_product,sales');
INSERT INTO ratings VALUES (6, 0, 7, 'avg_rating_per_product,sales');
INSERT INTO ratings VALUES (4, 1, 7, '');
INSERT INTO ratings VALUES (1, 4, 1, 'avg_rating_per_product');
INSERT INTO ratings VALUES (4, 1, 5, 'ratings,avg_sales_per_product,sales');
INSERT INTO ratings VALUES (7, 4, 5, '');
INSERT INTO ratings VALUES (0, 4, 6, 'ratings,sales');
INSERT INTO ratings VALUES (3, 3, 7, 'avg_rating_per_product,avg_sales_per_product,sales');
INSERT INTO ratings VALUES (1, 2, 2, 'ratings,avg_sales_per_product,sales');
INSERT INTO ratings VALUES (3, 0, 4, 'ratings,avg_sales_per_product,sales');
INSERT INTO ratings VALUES (6, 2, 4, '');
INSERT INTO ratings VALUES (5, 2, 5, 'avg_sales_per_product');
INSERT INTO ratings VALUES (9, 2, 5, '');
-- INSERT INTO ratings VALUES (6, 0, 9, 'avg_rating_per_product,avg_sales_per_product,sales');
INSERT INTO ratings VALUES (4, 1, 8, 'avg_rating_per_product,avg_sales_per_product');
INSERT INTO ratings VALUES (5, 4, 7, 'ratings,avg_sales_per_product');
INSERT INTO ratings VALUES (8, 1, 5, '');
INSERT INTO ratings VALUES (0, 3, 4, 'ratings');
INSERT INTO ratings VALUES (4, 3, 8, 'ratings,avg_rating_per_product,avg_sales_per_product');
INSERT INTO ratings VALUES (0, 0, 7, 'avg_rating_per_product');
INSERT INTO ratings VALUES (7, 1, 0, 'ratings');
INSERT INTO ratings VALUES (1, 4, 5, '');
INSERT INTO ratings VALUES (1, 4, 0, 'ratings,avg_rating_per_product,avg_sales_per_product');
INSERT INTO ratings VALUES (6, 1, 6, 'avg_sales_per_product,sales');
INSERT INTO ratings VALUES (7, 2, 3, 'ratings,sales');
INSERT INTO ratings VALUES (9, 1, 2, '');
INSERT INTO ratings VALUES (4, 0, 4, 'avg_rating_per_product,avg_sales_per_product,sales');
INSERT INTO ratings VALUES (5, 3, 5, 'avg_rating_per_product');
INSERT INTO ratings VALUES (8, 2, 3, 'ratings,avg_sales_per_product,sales');
INSERT INTO ratings VALUES (7, 4, 7, 'avg_sales_per_product,sales');
INSERT INTO ratings VALUES (0, 3, 7, 'sales');


select * from v1_ratings;
select * from v2_avg_rating_per_product;
select * from v3_avg_sales_per_product;
select * from v4_sales;
select * from v5_avg_sales_per_user;
