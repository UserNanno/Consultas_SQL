from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

class DocumentType(Enum):
   DNI = "DNI"
   CE = "CE"
   PASSPORT = "PASSPORT"
   RUC = "RUC"

@dataclass(frozen=True)
class PersonDocument:
   doc_type: DocumentType
   doc_number: str

@dataclass(frozen=True)
class ConsultaRequest:
   titular: PersonDocument
   incluir_conyuge: bool = False
   conyuge: Optional[PersonDocument] = None

@dataclass(frozen=True)
class ConsultaResult:
   out_xlsm: Path
   results_dir: Path
