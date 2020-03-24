ALTER TABLE sessions DROP CONSTRAINT FKsessions800212;
ALTER TABLE "order" DROP CONSTRAINT FKorder8292;
ALTER TABLE "order" DROP CONSTRAINT FKorder585286;
ALTER TABLE products DROP CONSTRAINT FKproducts292977;
ALTER TABLE products DROP CONSTRAINT FKproducts732050;
ALTER TABLE products DROP CONSTRAINT FKproducts907628;
ALTER TABLE profile_previously_viewed DROP CONSTRAINT FKprofile_pr728803;
ALTER TABLE profile_previously_viewed DROP CONSTRAINT FKprofile_pr801267;
DROP TABLE IF EXISTS categories CASCADE;
DROP TABLE IF EXISTS doelgroepen CASCADE;
DROP TABLE IF EXISTS "order" CASCADE;
DROP TABLE IF EXISTS profiles CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS sessions CASCADE;
DROP TABLE IF EXISTS brands CASCADE;
DROP TABLE IF EXISTS profile_previously_viewed CASCADE;
CREATE TABLE categories (ID varchar(255) NOT NULL, category varchar(255), subcategory varchar(255), subsubcategory varchar(255), PRIMARY KEY (ID));
CREATE TABLE doelgroepen (ID varchar(255) NOT NULL, target_audience varchar(255), PRIMARY KEY (ID));
CREATE TABLE "order" (sessionsID varchar(255) NOT NULL, productID varchar(255) NOT NULL, PRIMARY KEY (sessionsID));
CREATE TABLE profiles (ID varchar(255) NOT NULL, segment varchar(255), PRIMARY KEY (ID));
CREATE TABLE products (ID varchar(255) NOT NULL, doelgroepenID varchar(255) NOT NULL, categoriesID varchar(255) NOT NULL, brandID varchar(255) NOT NULL, name varchar(255), description varchar(400), herhaalaankopen bool, price int4, mcrp int4, inhoud varchar(255), discount int4, PRIMARY KEY (ID));
CREATE TABLE sessions (ID varchar(255) NOT NULL, profilesID varchar(255) NOT NULL, device_type varchar(255), PRIMARY KEY (ID));
CREATE TABLE brands (ID varchar(255) NOT NULL, brand varchar(255), PRIMARY KEY (ID));
CREATE TABLE profile_previously_viewed (profileID varchar(255) NOT NULL, productID varchar(255) NOT NULL);
ALTER TABLE sessions ADD CONSTRAINT FKsessions800212 FOREIGN KEY (profilesID) REFERENCES profiles (ID);
ALTER TABLE "order" ADD CONSTRAINT FKorder8292 FOREIGN KEY (sessionsID) REFERENCES sessions (ID);
ALTER TABLE "order" ADD CONSTRAINT FKorder585286 FOREIGN KEY (productID) REFERENCES products (ID);
ALTER TABLE products ADD CONSTRAINT FKproducts292977 FOREIGN KEY (doelgroepenID) REFERENCES doelgroepen (ID);
ALTER TABLE products ADD CONSTRAINT FKproducts732050 FOREIGN KEY (categoriesID) REFERENCES categories (ID);
ALTER TABLE products ADD CONSTRAINT FKproducts907628 FOREIGN KEY (brandID) REFERENCES brands (ID);
ALTER TABLE profile_previously_viewed ADD CONSTRAINT FKprofile_pr728803 FOREIGN KEY (productID) REFERENCES products (ID);
ALTER TABLE profile_previously_viewed ADD CONSTRAINT FKprofile_pr801267 FOREIGN KEY (profileID) REFERENCES profiles (ID);