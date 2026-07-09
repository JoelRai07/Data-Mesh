import create_datamodel
import pipeline_default_to_staging
import pipeline_staging_to_audit
import pipeline_audit_to_target
import contract_check


def main():
    print("=" * 60)
    print("STUFE 0/4: Datenmodell (CREATE TABLE IF NOT EXISTS)")
    print("=" * 60)
    create_datamodel.main()

    print("\n" + "=" * 60)
    print("STUFE 1/4: source -> staging")
    print("=" * 60)
    pipeline_default_to_staging.main()

    print("\n" + "=" * 60)
    print("STUFE 2/4: staging -> audit")
    print("=" * 60)
    pipeline_staging_to_audit.main()

    print("\n" + "=" * 60)
    print("STUFE 3/4: audit -> target")
    print("=" * 60)
    pipeline_audit_to_target.main()

    print("\n" + "=" * 60)
    print("STUFE 4/4: Data Contract durchsetzen (Publish-Gate)")
    print("=" * 60)
    contract_check.main()

    print("\n" + "=" * 60)
    print("Gesamter Pipeline-Lauf abgeschlossen.")
    print("=" * 60)


if __name__ == "__main__":
    main()
