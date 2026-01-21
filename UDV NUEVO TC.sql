begin
    declare v_dia int default 0;
    declare v_mes int default 202401;
    declare v_sqlStr string;
 
    bucle: loop
 
        set v_sqlStr = '
        insert into catalog_lhcl_prod_bcp.bcp_edv_coecnm.t52537_tp_analisispushCTR
        (codmes,fecevncio,ctddiasspostaccion,ctdcomunicacionexitosa,ctdcomunicacionleidos)
        select 
            a.fecprogramadavionemensajeinstantaneo codmes,
            a.fecprogramadavionemensajeinstantaneo fecevncio,
            '||v_dia||' as ctddiasspostaccion,
            ctdcomunicacionexitosa,
            sum(case when b.ctdacciones is not null then 1 else 0 end) ctdcomunicacionleidos
        from catalog_lhcl_prod_bcp.bcp_udv_int_yu_h_envionemensajeinstantaneo a
        left join
        (
            select fecprogramadavionemensajeinstantaneo,
                   codenvionemensajeinstantaneo,
                   count(1) ctdacciones
            from catalog_lhcl_prod_bcp.bcp_udv_int_yu_h_accionenvionemensajeinstantaneo
            where to_char(fecprogramadavionemensajeinstantaneo,''yyyyMM'')='''||v_mes||''' 
              and tipaccionevionicomunicacion=''10'' 
              and date_diff(month,feceaccionenvionemensajeinstantaneo,fecprogramadavionemensajeinstantaneo) >= 0 
              and date_diff(day,feceaccionenvionemensajeinstantaneo,fecprogramadavionemensajeinstantaneo) <= '||v_dia||'
            group by fecprogramadavionemensajeinstantaneo,codenvionemensajeinstantaneo
        ) b
        on (a.fecprogramadavionemensajeinstantaneo=b.fecprogramadavionemensajeinstantaneo 
            and a.codenvionemensajeinstantaneo=b.codenvionemensajeinstantaneo)
        where a.codmesprogramadavionemensajeinstantaneo='''||v_mes||''' 
          and a.codcanalcomunicacion in (''A1'',''A2'') 
          and a.tipestadoenviocomunicacion=''1''
        group by a.codmesprogramadavionemensajeinstantaneo,
                 a.fecprogramadavionemensajeinstantaneo';
 
        execute immediate v_sqlStr;
 
        set v_dia = v_dia + 1;
 
        if v_dia >= 60 then
            leave bucle;
        end if;
 
        if v_dia < 60 then
            iterate bucle;
        end if;
 
    end loop bucle;
 
end;
