CREATE TABLE IF NOT EXISTS material (
	id	serial primary key,
	name	text
);
CREATE TABLE IF NOT EXISTS bim_handle (
	id	serial primary key,
	bim_handle_type	text
);
CREATE TABLE IF NOT EXISTS description (
	id	serial primary key,
	source_type	text,
	source_id	text,
	category	text,
	family	text,
	element_type	text,
	bim_handle_id	integer
);
CREATE TABLE IF NOT EXISTS raw_data (
	id	serial primary key,
	key	text,
	storage_type	text,
	double_value	double precision,
	string_value	text,
	integer_value	integer
);

CREATE TABLE IF NOT EXISTS thermal_data_map (
	material_id	integer references material(id),
	raw_data_id	integer references raw_data(id)
);
CREATE TABLE IF NOT EXISTS structural_data_map (
	material_id	integer references material(id),
	raw_data_id	integer references raw_data(id)
);

CREATE TABLE IF NOT EXISTS material_map (
	bim_handle_id	integer references bim_handle(id),
	material_id	integer references material(id)
);

CREATE TABLE IF NOT EXISTS geometry_item (
	id	serial primary key,
	geometry_type	text,
	data	text,
	bim_handle_id	integer references bim_handle(id)
);
CREATE TABLE IF NOT EXISTS description_data_map (
	description_id	integer references description(id),
	raw_data_id	integer references raw_data(id)
);

CREATE TABLE IF NOT EXISTS bimdocuments  (
	id	serial primary key
);

CREATE TABLE IF NOT EXISTS bim_rel (
	main_id	integer,
	related_id	integer,
	bim_rel_type	text,
    bimbocuments_id integer references bimdocuments(id)
);
CREATE TABLE IF NOT EXISTS bim_meta (
	version	text
);



