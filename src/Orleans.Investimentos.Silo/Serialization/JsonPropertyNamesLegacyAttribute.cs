namespace Orleans.Investimentos.Silo.Serialization;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public sealed class JsonPropertyNamesLegacyAttribute(params string[] legacyNames) : Attribute
{
    public string[] LegacyNames { get; } = legacyNames;
}
