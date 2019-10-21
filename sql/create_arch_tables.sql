DO $$

BEGIN
   FOR counter IN 1..100 LOOP
		EXECUTE 'CREATE TABLE public.z_arc' || counter ||' (
					timestamp timestamp without time zone NOT NULL,
					p_01 double precision,
					s_01 numeric(22,0),
					primary key (timestamp)
				)';

	   RAISE NOTICE 'Counter: %', counter;
   END LOOP;
END;$$
