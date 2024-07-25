from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import Column, String, BIGINT, Integer, ForeignKey, JSON, Numeric
from infra.db.settings.base import Base


class Tercerizados(Base):
    __tablename__ = "tercerizados"

    id_terc = Column(String, primary_key=True)
    sg_orgao_sup_tabela_ug = Column(String)
    cd_ug_gestora = Column(String)
    nm_ug_tabela_ug = Column(String)
    sg_ug_gestora = Column(String)
    nr_contrato = Column(String)
    nr_cnpj = Column(String)
    nm_razao_social = Column(String)
    nr_cpf = Column(String)
    nm_terceirizado = Column(String)
    nm_categoria_profissional = Column(String)
    nm_escolaridade = Column(String)
    nr_jornada = Column(Integer)
    nm_unidade_prestacao = Column(String)
    vl_mensal_salario = Column(Numeric(precision=10, scale=2))
    vl_mensal_custo = Column(Numeric(precision=10, scale=2))
    Num_Mes_Carga = Column(Integer)
    Mes_Carga = Column(String)
    Ano_Carga = Column(Integer)
    sg_orgao = Column(String)
    nm_orgao = Column(String)
    cd_orgao_siafi = Column(String)
    cd_orgao_siape = Column(String)

    def __repr__(self):
        return (f"Tercerizados(id_terc={self.id_terc}, sg_orgao_sup_tabela_ug={self.sg_orgao_sup_tabela_ug}, "
                f"cd_ug_gestora={self.cd_ug_gestora}, nm_ug_tabela_ug={self.nm_ug_tabela_ug}, "
                f"sg_ug_gestora={self.sg_ug_gestora}, nr_contrato={self.nr_contrato}, nr_cnpj={self.nr_cnpj}, "
                f"nm_razao_social={self.nm_razao_social}, nr_cpf={self.nr_cpf}, nm_terceirizado={self.nm_terceirizado}, "
                f"nm_categoria_profissional={self.nm_categoria_profissional}, nm_escolaridade={self.nm_escolaridade}, "
                f"nr_jornada={self.nr_jornada}, nm_unidade_prestacao={self.nm_unidade_prestacao}, "
                f"vl_mensal_salario={self.vl_mensal_salario}, vl_mensal_custo={self.vl_mensal_custo}, "
                f"Num_Mes_Carga={self.Num_Mes_Carga}, Mes_Carga={self.Mes_Carga}, Ano_Carga={self.Ano_Carga}, "
                f"sg_orgao={self.sg_orgao}, nm_orgao={self.nm_orgao}, cd_orgao_siafi={self.cd_orgao_siafi}, "
                f"cd_orgao_siape={self.cd_orgao_siape})")
